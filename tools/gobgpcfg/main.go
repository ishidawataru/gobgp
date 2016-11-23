package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/docopt/docopt-go"
	"github.com/ghodss/yaml"
	"github.com/libgit2/git2go"
	gobgp "github.com/osrg/gobgp/client"
	"github.com/osrg/gobgp/config"
	"github.com/spf13/viper"
)

var client *gobgp.GoBGPClient

type deltaType int

const (
	added deltaType = iota
	deleted
	updated
)

type delta struct {
	resource gobgp.Resource
	typ      git.Delta
}

func (d *delta) apply(c *gobgp.GoBGPClient) error {
	switch d.typ {
	case git.DeltaAdded:
		return d.resource.Create(c)
	case git.DeltaDeleted:
		return d.resource.Delete(c)
	case git.DeltaModified:
		return d.resource.Update(c)
	}
	return nil
}

type deltas []*delta

func (d deltas) Len() int {
	return len(d)
}

func (d deltas) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
}

func (d deltas) Less(i, j int) bool {
	if d[i].resource.Type() == d[j].resource.Type() {
		return d[i].typ < d[j].typ
	}
	return d[i].resource.Type() < d[j].resource.Type()
}

func newResourceFromOid(repo *git.Repository, oid *git.Oid) (gobgp.Resource, error) {
	blob, err := repo.LookupBlob(oid)
	if err != nil {
		return nil, err
	}
	return gobgp.NewResourceFromBytes(blob.Contents())
}

func newDeltasFromDiff(repo *git.Repository, diff *git.Diff) (deltas, error) {

	ds := deltas{}

	err := diff.ForEach(func(d git.DiffDelta, f float64) (git.DiffForEachHunkCallback, error) {
		var oid, oldOid *git.Oid
		switch d.Status {
		case git.DeltaAdded:
			oid = d.NewFile.Oid
		case git.DeltaDeleted:
			oid = d.OldFile.Oid
		case git.DeltaModified:
			oid = d.NewFile.Oid
			oldOid = d.OldFile.Oid
		default:
			return nil, fmt.Errorf("unsupport delta type: %v", d.Status)
		}

		r, err := newResourceFromOid(repo, oid)
		if err != nil {
			return nil, err
		}

		if d.Status == git.DeltaModified {
			l, err := newResourceFromOid(repo, oldOid)
			if err != nil {
				return nil, err
			}
			if r.Key() != l.Key() {
				return nil, fmt.Errorf("invalid modification: key is changed from %s to %s", l.Key(), r.Key())
			}
		}

		ds = append(ds, &delta{
			typ:      d.Status,
			resource: r,
		})

		return nil, nil

	}, git.DiffDetailFiles)

	if err != nil {
		return nil, err
	}

	sort.Sort(sort.Reverse(ds))

	return ds, nil
}

func treeFromTag(repo *git.Repository, tag string) (*git.Tree, error) {
	iter, err := repo.NewReferenceIteratorGlob("refs/tags/" + tag)
	if err != nil {
		return nil, err
	}
	ref, err := iter.Next()
	if err != nil {
		return nil, err
	}
	t, err := ref.Peel(git.ObjectTree)
	if err != nil {
		return nil, err
	}
	return t.AsTree()
}

func treeFromBranch(repo *git.Repository, branch string) (*git.Tree, error) {
	b, err := repo.LookupBranch(branch, git.BranchLocal)
	if err != nil {
		return nil, err
	}
	t, err := b.Peel(git.ObjectTree)
	if err != nil {
		return nil, err
	}
	return t.AsTree()
}

func commitFromBranch(repo *git.Repository, branch string) (*git.Commit, error) {
	b, err := repo.LookupBranch(branch, git.BranchLocal)
	if err != nil {
		return nil, err
	}
	t, err := b.Peel(git.ObjectCommit)
	if err != nil {
		return nil, err
	}
	return t.AsCommit()
}

func Apply(args map[string]interface{}) error {

	dir := args["--config-dir"].(string)
	br := args["--branch"].(string)
	tag := args["--tag"].(string)

	repo, err := git.OpenRepository(dir)
	if err != nil {
		return err
	}

	t1, _ := treeFromTag(repo, tag)

	// ask user if it's ok to proceed

	t2, err := treeFromBranch(repo, br)
	if err != nil {
		return err
	}

	diff, err := repo.DiffTreeToTree(t1, t2, nil)
	if err != nil {
		return err
	}

	ds, err := newDeltasFromDiff(repo, diff)
	if err != nil {
		return err
	}

	for _, r := range ds {
		if err = r.apply(client); err != nil {
			j, _ := json.Marshal(r.resource)
			y, _ := yaml.JSONToYAML(j)
			fmt.Println(string(y))
			return err
		}
	}

	commit, err := commitFromBranch(repo, br)
	if err != nil {
		return err
	}

	// move tag:latest to HEAD of the branch
	_, err = repo.Tags.CreateLightweight(tag, commit, true)
	return err
}

func dumpYAML(r gobgp.Resource, name string) error {
	j, err := json.Marshal(r)
	if err != nil {
		return err
	}
	y, err := yaml.JSONToYAML(j)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(name, y, 0666)
}

func Migrate(args map[string]interface{}) error {
	file := args["<config-file>"].(string)
	typ := args["--config-type"].(string)
	v := viper.New()
	v.SetConfigFile(file)
	v.SetConfigType(typ)
	if err := v.ReadInConfig(); err != nil {
		return err
	}
	c := &config.BgpConfigSet{}
	if err := v.UnmarshalExact(c); err != nil {
		return err
	}
	rs, _ := gobgp.NewResourcesFromConfig(c)
	for _, r := range rs {
		if err := dumpYAML(r, fmt.Sprintf("%s.%s", r.Key(), typ)); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	usage := `gobgpcfg
Usage:
    gobgpcfg [options] apply
    gobgpcfg [options] migrate <config-file>

Options:
    --config-dir=<dir>        Directory which has gobgp configuration [default: /etc/gobgp]
    --branch=<branch>         Branch to apply [default: master]
    --tag=<tag>               Tag which points the last apply commit [default: latest]
    --dry-run                 Dry run.
    --config-type=<type>      Configuration format [default: yaml]`

	args, err := docopt.Parse(usage, nil, true, "", false)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if args["apply"].(bool) && !args["--dry-run"].(bool) {
		client, err = gobgp.NewGoBGPClient("")
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer client.Close()
	}

	switch {
	case args["apply"].(bool):
		err = Apply(args)
	case args["migrate"].(bool):
		err = Migrate(args)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
