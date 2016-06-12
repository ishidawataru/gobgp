// Copyright (C) 2015 Nippon Telegraph and Telephone Corporation.
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

package server

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	api "github.com/osrg/gobgp/api"
	"github.com/osrg/gobgp/packet/bgp"
	"github.com/osrg/gobgp/table"
	"github.com/osrg/gobgp/zebra"
	"gopkg.in/tomb.v2"
	"net"
	"strconv"
	"strings"
)

func newIPRouteMessage(dst []*table.Path) (zebra.API_TYPE, *zebra.IPRouteBody) {
	var command zebra.API_TYPE
	paths := make([]*table.Path, 0, len(dst))
	for _, path := range dst {
		if path == nil || path.IsFromExternal() {
			continue
		}
		paths = append(paths, path)
	}
	if len(paths) == 0 {
		return command, nil
	}
	path := paths[0]

	l := strings.SplitN(path.GetNlri().String(), "/", 2)
	var prefix net.IP
	nexthops := make([]net.IP, 0, len(paths))
	switch path.GetRouteFamily() {
	case bgp.RF_IPv4_UC:
		if path.IsWithdraw == true {
			command = zebra.IPV4_ROUTE_DELETE
		} else {
			command = zebra.IPV4_ROUTE_ADD
		}
		prefix = net.ParseIP(l[0]).To4()
		for _, p := range paths {
			nexthops = append(nexthops, p.GetNexthop().To4())
		}
	case bgp.RF_IPv6_UC:
		if path.IsWithdraw == true {
			command = zebra.IPV6_ROUTE_DELETE
		} else {
			command = zebra.IPV6_ROUTE_ADD
		}
		prefix = net.ParseIP(l[0]).To16()
		for _, p := range paths {
			nexthops = append(nexthops, p.GetNexthop().To16())
		}
	default:
		return command, nil
	}
	flags := uint8(zebra.MESSAGE_NEXTHOP)
	plen, _ := strconv.Atoi(l[1])
	med, err := path.GetMed()
	if err == nil {
		flags |= zebra.MESSAGE_METRIC
	}
	return command, &zebra.IPRouteBody{
		Type:         zebra.ROUTE_BGP,
		SAFI:         zebra.SAFI_UNICAST,
		Message:      flags,
		Prefix:       prefix,
		PrefixLength: uint8(plen),
		Nexthops:     nexthops,
		Metric:       med,
	}
}

func createRequestFromIPRouteMessage(m *zebra.Message) *api.AddPathRequest {

	header := m.Header
	body := m.Body.(*zebra.IPRouteBody)
	family := bgp.RF_IPv6_UC
	if header.Command == zebra.IPV4_ROUTE_ADD || header.Command == zebra.IPV4_ROUTE_DELETE {
		family = bgp.RF_IPv4_UC
	}

	var nlri bgp.AddrPrefixInterface
	pattr := make([]bgp.PathAttributeInterface, 0)
	var mpnlri *bgp.PathAttributeMpReachNLRI
	var isWithdraw bool = header.Command == zebra.IPV4_ROUTE_DELETE || header.Command == zebra.IPV6_ROUTE_DELETE

	origin := bgp.NewPathAttributeOrigin(bgp.BGP_ORIGIN_ATTR_TYPE_IGP)
	pattr = append(pattr, origin)

	log.WithFields(log.Fields{
		"Topic":        "Zebra",
		"RouteType":    body.Type.String(),
		"Flag":         body.Flags.String(),
		"Message":      body.Message,
		"Prefix":       body.Prefix,
		"PrefixLength": body.PrefixLength,
		"Nexthop":      body.Nexthops,
		"IfIndex":      body.Ifindexs,
		"Metric":       body.Metric,
		"Distance":     body.Distance,
		"api":          header.Command.String(),
	}).Debugf("create path from ip route message.")

	switch family {
	case bgp.RF_IPv4_UC:
		nlri = bgp.NewIPAddrPrefix(body.PrefixLength, body.Prefix.String())
		nexthop := bgp.NewPathAttributeNextHop(body.Nexthops[0].String())
		pattr = append(pattr, nexthop)
	case bgp.RF_IPv6_UC:
		nlri = bgp.NewIPv6AddrPrefix(body.PrefixLength, body.Prefix.String())
		mpnlri = bgp.NewPathAttributeMpReachNLRI(body.Nexthops[0].String(), []bgp.AddrPrefixInterface{nlri})
		pattr = append(pattr, mpnlri)
	default:
		log.WithFields(log.Fields{
			"Topic": "Zebra",
		}).Errorf("unsupport address family: %s", family)
		return nil
	}

	med := bgp.NewPathAttributeMultiExitDisc(body.Metric)
	pattr = append(pattr, med)

	binPattrs := make([][]byte, 0, len(pattr))
	for _, a := range pattr {
		bin, _ := a.Serialize()
		binPattrs = append(binPattrs, bin)
	}

	binNlri, _ := nlri.Serialize()

	path := &api.Path{
		Nlri:           binNlri,
		Pattrs:         binPattrs,
		IsWithdraw:     isWithdraw,
		Family:         uint32(family),
		IsFromExternal: true,
	}
	return &api.AddPathRequest{
		Resource: api.Resource_GLOBAL,
		Path:     path,
	}

}

type zebraWatcher struct {
	t      tomb.Tomb
	ch     chan watcherEvent
	client *zebra.Client
	apiCh  chan *GrpcRequest
}

func (w *zebraWatcher) notify(t watcherEventType) chan watcherEvent {
	if t == WATCHER_EVENT_BESTPATH_CHANGE {
		return w.ch
	}
	return nil
}

func (w *zebraWatcher) stop() {
	w.t.Kill(nil)
}

func (w *zebraWatcher) watchingEventTypes() []watcherEventType {
	return []watcherEventType{WATCHER_EVENT_BESTPATH_CHANGE}
}

func (w *zebraWatcher) loop() error {
	for {
		select {
		case <-w.t.Dying():
			return w.client.Close()
		case msg := <-w.client.Receive():
			switch msg.Body.(type) {
			case *zebra.IPRouteBody:
				p := createRequestFromIPRouteMessage(msg)
				if p != nil {
					ch := make(chan *GrpcResponse)
					w.apiCh <- &GrpcRequest{
						RequestType: REQ_ADD_PATH,
						Data:        p,
						ResponseCh:  ch,
					}
					if err := (<-ch).Err(); err != nil {
						log.Errorf("failed to add path from zebra: %s", p)
					}
				}
			}
		case ev := <-w.ch:
			msg := ev.(*watcherEventBestPathMsg)
			if table.UseMultiplePaths.Enabled {
				for _, dst := range msg.multiPathList {
					if typ, m := newIPRouteMessage(dst); m != nil {
						w.client.SendCommand(typ, m)
					}
				}
			} else {
				for _, path := range msg.pathList {
					if typ, m := newIPRouteMessage([]*table.Path{path}); m != nil {
						w.client.SendCommand(typ, m)
					}
				}
			}
		}
	}
	return nil
}

func newZebraWatcher(apiCh chan *GrpcRequest, url string, protos []string, version uint8) (*zebraWatcher, error) {
	l := strings.SplitN(url, ":", 2)
	if len(l) != 2 {
		return nil, fmt.Errorf("unsupported url: %s", url)
	}
	cli, err := zebra.NewClient(l[0], l[1], zebra.ROUTE_BGP, version)
	if err != nil {
		return nil, err
	}
	cli.SendHello()
	cli.SendRouterIDAdd()
	cli.SendInterfaceAdd()
	for _, typ := range protos {
		t, err := zebra.RouteTypeFromString(typ)
		if err != nil {
			return nil, err
		}
		cli.SendRedistribute(t)
	}
	w := &zebraWatcher{
		ch:     make(chan watcherEvent),
		client: cli,
		apiCh:  apiCh,
	}
	w.t.Go(w.loop)
	return w, nil
}
