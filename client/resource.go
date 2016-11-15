// Copyright (C) 2016 Nippon Telegraph and Telephone Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client provides a wrapper for GoBGP's gRPC API
package client

import (
	"fmt"

	"github.com/ghodss/yaml"
	"github.com/osrg/gobgp/config"
	"github.com/osrg/gobgp/table"
)

type ResourceType int

const (
	ResourceNeighbor ResourceType = iota
	ResourcePolicy
	ResourcePrefixSet
	ResourceNeighborSet
	ResourceASPathSet
	ResourceCommunitySet
	ResourceExtCommunitySet
	ResourceLargeCommunitySet
	ResourceGlobal
)

func (t ResourceType) String() string {
	switch t {
	case ResourceNeighbor:
		return "neighbor"
	case ResourcePolicy:
		return "policy"
	case ResourcePrefixSet:
		return "prefix-set"
	case ResourceNeighborSet:
		return "neighbor-set"
	case ResourceASPathSet:
		return "as-path-set"
	case ResourceCommunitySet:
		return "community-set"
	case ResourceExtCommunitySet:
		return "ext-community-set"
	case ResourceLargeCommunitySet:
		return "large-community-set"
	case ResourceGlobal:
		return "global"
	}
	return ""
}

type Resource interface {
	Create(*GoBGPClient) error
	Delete(*GoBGPClient) error
	Update(*GoBGPClient) error

	// TODO
	// apply(*GoBGPClient) error
	// list(*GoBGPClient) ([]Resource, error)

	Type() ResourceType
	Key() string
}

type typeMetadata struct {
	Kind string `json:"kind"`
}

func (m *typeMetadata) Type() ResourceType {
	switch m.Kind {
	case "global":
		return ResourceGlobal
	case "neighbor":
		return ResourceNeighbor
	case "policy":
		return ResourcePolicy
	case "prefix-set":
		return ResourcePrefixSet
	case "neighbor-set":
		return ResourceNeighborSet
	case "as-path-set":
		return ResourceASPathSet
	case "community-set":
		return ResourceCommunitySet
	case "ext-community-set":
		return ResourceExtCommunitySet
	case "large-community-set":
		return ResourceLargeCommunitySet
	}
	return ResourceType(-1)
}

type globalResource struct {
	typeMetadata
	Spec config.Global `json:"spec,omitempty"`
}

func (r *globalResource) Create(cli *GoBGPClient) error {
	return cli.StartServer(&r.Spec)
}

func (r *globalResource) Delete(cli *GoBGPClient) error {
	return cli.StopServer()
}

func (r *globalResource) Update(cli *GoBGPClient) error {
	return fmt.Errorf("unsupported")
}

func (r *globalResource) Key() string {
	return "global"
}

type neighborResource struct {
	typeMetadata
	Spec config.Neighbor `json:"spec,omitempty"`
}

func (r *neighborResource) Create(cli *GoBGPClient) error {
	return cli.AddNeighbor(&r.Spec)
}

func (r *neighborResource) Delete(cli *GoBGPClient) error {
	return cli.DeleteNeighbor(&r.Spec)
}

func (r *neighborResource) Update(cli *GoBGPClient) error {
	return fmt.Errorf("unsupported")
}

func (r *neighborResource) Key() string {
	return r.Spec.Config.NeighborAddress
}

type policyResource struct {
	typeMetadata
	Spec config.PolicyDefinition `json:"spec,omitempty"`
}

func (r *policyResource) Create(cli *GoBGPClient) error {
	p, err := table.NewPolicy(r.Spec)
	if err != nil {
		return err
	}
	return cli.AddPolicy(p, false)
}

func (r *policyResource) Delete(cli *GoBGPClient) error {
	p, err := table.NewPolicy(r.Spec)
	if err != nil {
		return err
	}
	return cli.DeletePolicy(p, true, false)
}

func (r *policyResource) Update(cli *GoBGPClient) error {
	p, err := table.NewPolicy(r.Spec)
	if err != nil {
		return err
	}
	return cli.ReplacePolicy(p, false, false)
}

func (r *policyResource) Key() string {
	return r.Spec.Name
}

type definedSetResource struct {
	typeMetadata
}

func (r *definedSetResource) create(cli *GoBGPClient, spec config.DefinedSets) error {
	d, err := table.NewDefinedSet(spec)
	if err != nil {
		return err
	}
	return cli.AddDefinedSet(d)
}

func (r *definedSetResource) delete(cli *GoBGPClient, spec config.DefinedSets) error {
	d, err := table.NewDefinedSet(spec)
	if err != nil {
		return err
	}
	return cli.DeleteDefinedSet(d, true)
}

func (r *definedSetResource) update(cli *GoBGPClient, spec config.DefinedSets) error {
	d, err := table.NewDefinedSet(spec)
	if err != nil {
		return err
	}
	return cli.ReplaceDefinedSet(d)
}

func (r *definedSetResource) key(spec config.DefinedSets) string {
	d, _ := table.NewDefinedSet(spec)
	return d.Name()
}

type prefixSetResource struct {
	definedSetResource
	Spec config.PrefixSet `json:"spec,omitempty"`
}

func (r *prefixSetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{PrefixSets: []config.PrefixSet{r.Spec}}
}

func (r *prefixSetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *prefixSetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *prefixSetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *prefixSetResource) Key() string {
	return r.key(r.definedSet())
}

type neighborSetResource struct {
	definedSetResource
	Spec config.NeighborSet `json:"spec,omitempty"`
}

func (r *neighborSetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{NeighborSets: []config.NeighborSet{r.Spec}}
}

func (r *neighborSetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *neighborSetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *neighborSetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *neighborSetResource) Key() string {
	return r.key(r.definedSet())
}

type communitySetResource struct {
	definedSetResource
	Spec config.CommunitySet `json:"spec,omitempty"`
}

func (r *communitySetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{BgpDefinedSets: config.BgpDefinedSets{
		CommunitySets: []config.CommunitySet{r.Spec}},
	}
}

func (r *communitySetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *communitySetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *communitySetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *communitySetResource) Key() string {
	return r.key(r.definedSet())
}

type asPathSetResource struct {
	definedSetResource
	Spec config.AsPathSet `json:"spec,omitempty"`
}

func (r *asPathSetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{BgpDefinedSets: config.BgpDefinedSets{
		AsPathSets: []config.AsPathSet{r.Spec}},
	}
}

func (r *asPathSetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *asPathSetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *asPathSetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *asPathSetResource) Key() string {
	return r.key(r.definedSet())
}

type extCommunitySetResource struct {
	definedSetResource
	Spec config.ExtCommunitySet `json:"spec,omitempty"`
}

func (r *extCommunitySetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{BgpDefinedSets: config.BgpDefinedSets{
		ExtCommunitySets: []config.ExtCommunitySet{r.Spec}},
	}
}

func (r *extCommunitySetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *extCommunitySetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *extCommunitySetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *extCommunitySetResource) Key() string {
	return r.key(r.definedSet())
}

type largeCommunitySetResource struct {
	definedSetResource
	Spec config.LargeCommunitySet `json:"spec,omitempty"`
}

func (r *largeCommunitySetResource) definedSet() config.DefinedSets {
	return config.DefinedSets{BgpDefinedSets: config.BgpDefinedSets{
		LargeCommunitySets: []config.LargeCommunitySet{r.Spec}},
	}
}

func (r *largeCommunitySetResource) Create(cli *GoBGPClient) error {
	return r.create(cli, r.definedSet())
}

func (r *largeCommunitySetResource) Delete(cli *GoBGPClient) error {
	return r.delete(cli, r.definedSet())
}

func (r *largeCommunitySetResource) Update(cli *GoBGPClient) error {
	return r.update(cli, r.definedSet())
}

func (r *largeCommunitySetResource) Key() string {
	return r.key(r.definedSet())
}

func NewResourceFromBytes(b []byte) (Resource, error) {
	m := &typeMetadata{}
	err := yaml.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}
	var r Resource
	switch m.Kind {
	case "global":
		r = &globalResource{}
	case "neighbor":
		r = &neighborResource{}
	case "policy":
		r = &policyResource{}
	case "prefix-set":
		r = &prefixSetResource{}
	case "neighbor-set":
		r = &neighborSetResource{}
	case "as-path-set":
		r = &asPathSetResource{}
	case "community-set":
		r = &communitySetResource{}
	case "ext-community-set":
		r = &extCommunitySetResource{}
	case "large-community-set":
		r = &largeCommunitySetResource{}
	default:
		return nil, fmt.Errorf("failed to create resource from bytes: %s", m.Kind)
	}

	if err := yaml.Unmarshal(b, r); err != nil {
		return nil, err
	}
	return r, nil
}

func NewResourcesFromConfig(c *config.BgpConfigSet) ([]Resource, error) {
	r := []Resource{}
	r = append(r, &globalResource{
		typeMetadata: typeMetadata{
			Kind: "global",
		},
		Spec: c.Global,
	})
	for _, n := range c.Neighbors {
		r = append(r, &neighborResource{
			typeMetadata: typeMetadata{
				Kind: "neighbor",
			},
			Spec: n,
		})
	}
	for _, d := range c.DefinedSets.PrefixSets {
		r = append(r, &prefixSetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "prefix-set",
				},
			},
			Spec: d,
		})
	}
	for _, d := range c.DefinedSets.NeighborSets {
		r = append(r, &neighborSetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "neighbor-set",
				},
			},
			Spec: d,
		})
	}
	for _, d := range c.DefinedSets.BgpDefinedSets.CommunitySets {
		r = append(r, &communitySetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "community-set",
				},
			},
			Spec: d,
		})
	}
	for _, d := range c.DefinedSets.BgpDefinedSets.ExtCommunitySets {
		r = append(r, &extCommunitySetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "ext-community-set",
				},
			},
			Spec: d,
		})
	}
	for _, d := range c.DefinedSets.BgpDefinedSets.AsPathSets {
		r = append(r, &asPathSetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "as-path-set",
				},
			},
			Spec: d,
		})
	}
	for _, d := range c.DefinedSets.BgpDefinedSets.LargeCommunitySets {
		r = append(r, &largeCommunitySetResource{
			definedSetResource: definedSetResource{
				typeMetadata: typeMetadata{
					Kind: "large-community-set",
				},
			},
			Spec: d,
		})
	}
	for _, p := range c.PolicyDefinitions {
		r = append(r, &policyResource{
			typeMetadata: typeMetadata{
				Kind: "policy",
			},
			Spec: p,
		})
	}
	return r, nil
}
