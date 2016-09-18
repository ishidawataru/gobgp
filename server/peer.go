// Copyright (C) 2014-2016 Nippon Telegraph and Telephone Corporation.
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
	"github.com/eapache/channels"
	"github.com/osrg/gobgp/config"
	"github.com/osrg/gobgp/packet/bgp"
	"github.com/osrg/gobgp/table"
	"net"
	"time"
)

const (
	FLOP_THRESHOLD    = time.Second * 30
	MIN_CONNECT_RETRY = 10
)

type Peer struct {
	tableId           string
	fsm               *FSM
	adjRibIn          *table.AdjRib
	adjRibOut         *table.AdjRib
	outgoing          *channels.InfiniteChannel
	policy            *table.RoutingPolicy
	localRib          *table.TableManager
	prefixLimitWarned map[bgp.RouteFamily]bool
}

func NewPeer(g *config.Global, conf *config.Neighbor, loc *table.TableManager, policy *table.RoutingPolicy) *Peer {
	peer := &Peer{
		outgoing:          channels.NewInfiniteChannel(),
		localRib:          loc,
		policy:            policy,
		fsm:               NewFSM(g, conf, policy),
		prefixLimitWarned: make(map[bgp.RouteFamily]bool),
	}
	if peer.isRouteServerClient() {
		peer.tableId = conf.Config.NeighborAddress
	} else {
		peer.tableId = table.GLOBAL_RIB_NAME
	}
	rfs, _ := config.AfiSafis(conf.AfiSafis).ToRfList()
	peer.adjRibIn = table.NewAdjRib(peer.ID(), rfs)
	peer.adjRibOut = table.NewAdjRib(peer.ID(), rfs)
	return peer
}

func (peer *Peer) ID() string {
	return peer.fsm.pConf.Config.NeighborAddress
}

func (peer *Peer) TableID() string {
	return peer.tableId
}

func (peer *Peer) isIBGPPeer() bool {
	return peer.fsm.pConf.Config.PeerAs == peer.fsm.gConf.Config.As
}

func (peer *Peer) isRouteServerClient() bool {
	return peer.fsm.pConf.RouteServer.Config.RouteServerClient
}

func (peer *Peer) isRouteReflectorClient() bool {
	return peer.fsm.pConf.RouteReflector.Config.RouteReflectorClient
}

func (peer *Peer) isGracefulRestartEnabled() bool {
	return peer.fsm.pConf.GracefulRestart.State.Enabled
}

func (peer *Peer) recvedAllEOR() bool {
	for _, a := range peer.fsm.pConf.AfiSafis {
		if s := a.MpGracefulRestart.State; s.Enabled && !s.EndOfRibReceived {
			return false
		}
	}
	return true
}

func (peer *Peer) configuredRFlist() []bgp.RouteFamily {
	rfs, _ := config.AfiSafis(peer.fsm.pConf.AfiSafis).ToRfList()
	return rfs
}

func (peer *Peer) forwardingPreservedFamilies() ([]bgp.RouteFamily, []bgp.RouteFamily) {
	list := []bgp.RouteFamily{}
	for _, a := range peer.fsm.pConf.AfiSafis {
		if s := a.MpGracefulRestart.State; s.Enabled && s.Received {
			f, _ := bgp.GetRouteFamily(string(a.Config.AfiSafiName))
			list = append(list, f)
		}
	}
	preserved := []bgp.RouteFamily{}
	notPreserved := []bgp.RouteFamily{}
	for _, f := range peer.configuredRFlist() {
		p := true
		for _, g := range list {
			if f == g {
				p = false
				preserved = append(preserved, f)
			}
		}
		if p {
			notPreserved = append(notPreserved, f)
		}
	}
	return preserved, notPreserved
}

func (peer *Peer) getAccepted(rfList []bgp.RouteFamily) []*table.Path {
	return peer.adjRibIn.PathList(rfList, true)
}

func (peer *Peer) filterpath(path *table.Path) *table.Path {
	if path == nil {
		return nil
	}
	if _, ok := peer.fsm.rfMap[path.GetRouteFamily()]; !ok {
		return nil
	}

	//iBGP handling
	if peer.isIBGPPeer() {
		ignore := false
		//RFC4684 Constrained Route Distribution
		if _, y := peer.fsm.rfMap[bgp.RF_RTC_UC]; y && path.GetRouteFamily() != bgp.RF_RTC_UC {
			ignore = true
			for _, ext := range path.GetExtCommunities() {
				for _, path := range peer.adjRibIn.PathList([]bgp.RouteFamily{bgp.RF_RTC_UC}, true) {
					rt := path.GetNlri().(*bgp.RouteTargetMembershipNLRI).RouteTarget
					if rt == nil {
						ignore = false
					} else if ext.String() == rt.String() {
						ignore = false
						break
					}
				}
				if !ignore {
					break
				}
			}
		}

		if !path.IsLocal() {
			ignore = true
			info := path.GetSource()
			//if the path comes from eBGP peer
			if info.AS != peer.fsm.pConf.Config.PeerAs {
				ignore = false
			}
			// RFC4456 8. Avoiding Routing Information Loops
			// A router that recognizes the ORIGINATOR_ID attribute SHOULD
			// ignore a route received with its BGP Identifier as the ORIGINATOR_ID.
			if id := path.GetOriginatorID(); peer.fsm.gConf.Config.RouterId == id.String() {
				log.WithFields(log.Fields{
					"Topic":        "Peer",
					"Key":          peer.ID(),
					"OriginatorID": id,
					"Data":         path,
				}).Debug("Originator ID is mine, ignore")
				return nil
			}
			if info.RouteReflectorClient {
				ignore = false
			}
			if peer.isRouteReflectorClient() {
				// RFC4456 8. Avoiding Routing Information Loops
				// If the local CLUSTER_ID is found in the CLUSTER_LIST,
				// the advertisement received SHOULD be ignored.
				for _, clusterId := range path.GetClusterList() {
					if clusterId.Equal(peer.fsm.peerInfo.RouteReflectorClusterID) {
						log.WithFields(log.Fields{
							"Topic":     "Peer",
							"Key":       peer.ID(),
							"ClusterID": clusterId,
							"Data":      path,
						}).Debug("cluster list path attribute has local cluster id, ignore")
						return nil
					}
				}
				ignore = false
			}
		}

		if ignore {

			for _, adv := range peer.adjRibOut.PathList([]bgp.RouteFamily{path.GetRouteFamily()}, false) {
				// we advertise a route from ebgp,
				// which is the old best. We got the
				// new best from ibgp. We don't
				// advertise the new best and need to
				// withdraw the old.
				if path.GetNlri().String() == adv.GetNlri().String() {
					return adv.Clone(true)
				}
			}
			log.WithFields(log.Fields{
				"Topic": "Peer",
				"Key":   peer.ID(),
				"Data":  path,
			}).Debug("From same AS, ignore.")
			return nil
		}
	}

	if peer.ID() == path.GetSource().Address.String() {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.ID(),
			"Data":  path,
		}).Debug("From me, ignore.")
		return nil
	}

	if !peer.isRouteServerClient() && isASLoop(peer, path) {
		return nil
	}

	path = path.Clone(path.IsWithdraw)
	path.UpdatePathAttrs(peer.fsm.gConf, peer.fsm.pConf)

	options := &table.PolicyOptions{
		Info: peer.fsm.peerInfo,
	}
	path = peer.policy.ApplyPolicy(peer.TableID(), table.POLICY_DIRECTION_EXPORT, path, options)

	// remove local-pref attribute
	// we should do this after applying export policy since policy may
	// set local-preference
	if path != nil && !peer.isIBGPPeer() && !peer.isRouteServerClient() {
		path.RemoveLocalPref()
	}
	return path
}

func (peer *Peer) getBestFromLocal(rfList []bgp.RouteFamily) ([]*table.Path, []*table.Path) {
	pathList := []*table.Path{}
	filtered := []*table.Path{}
	for _, path := range peer.localRib.GetBestPathList(peer.TableID(), rfList) {
		if p := peer.filterpath(path); p != nil {
			pathList = append(pathList, p)
		} else {
			filtered = append(filtered, path)
		}

	}
	if peer.isGracefulRestartEnabled() {
		for _, family := range rfList {
			pathList = append(pathList, table.NewEOR(family))
		}
	}
	return pathList, filtered
}

func (peer *Peer) isReadyToSend() bool {
	if peer.fsm.state != bgp.BGP_FSM_ESTABLISHED {
		return false
	}
	if peer.fsm.pConf.GracefulRestart.State.LocalRestarting {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.fsm.pConf.Config.NeighborAddress,
		}).Debug("now syncing, suppress sending updates")
		return false
	}
	return true
}

func (peer *Peer) extractOutgoingPaths(dsts []*table.Destination, withdrawals []*table.Path) []*table.Path {
	outgoing := make([]*table.Path, 0, len(dsts))
	for _, dst := range dsts {
		path := dst.GetNewBest(peer.TableID())
		// RFC4684 3.2. Intra-AS VPN Route Distribution
		// When processing RT membership NLRIs received from internal iBGP
		// peers, it is necessary to consider all available iBGP paths for a
		// given RT prefix, for building the outbound route filter, and **not just
		// the best path**.
		if path == nil && dst.Family() == bgp.RF_RTC_UC {
			old := dst.GetOldBest(peer.TableID())
			if old == nil || peer.adjRibOut.Exists(old) {
				continue
			}
			// we send a path even if it is not a best path
			for _, p := range dst.GetKnownPathList(peer.TableID()) {
				// just take care not to send back it
				if peer.ID() != p.GetSource().Address.String() {
					path = p
					break
				}
			}
		}
		if path == nil {
			continue
		}
		if peer.ID() == path.GetSource().Address.String() {
			// Say, gobgp was advertising prefix A and peer P also.
			// When gobgp withdraws prefix A, best path calculation chooses
			// the path from P as the best path for prefix A.
			// For peers other than P, this path should be advertised
			// (as implicit withdrawal). However for P, we should advertise
			// the local withdraw path.

			// Note: multiple paths having the same prefix could exist the
			// withdrawals list in the case of Route Server setup with
			// import policies modifying paths. In such case, gobgp sends
			// duplicated update messages; withdraw messages for the same
			// prefix.
			// However, currently we don't support local path for Route
			// Server setup so this is NOT the case.
			for _, w := range withdrawals {
				if w.IsLocal() && path.GetNlri().String() == w.GetNlri().String() {
					outgoing = append(outgoing, w)
					break
				}
			}
		} else {
			outgoing = append(outgoing, path)
		}
	}
	return outgoing
}

func (peer *Peer) processOutgoingPaths(paths []*table.Path) []*table.Path {
	outgoing := make([]*table.Path, 0, len(paths))
	for _, path := range paths {
		if p := peer.filterpath(path); p != nil {
			outgoing = append(outgoing, p)
		}
	}
	peer.adjRibOut.Update(outgoing)
	return outgoing
}

func (peer *Peer) handleRouteRefresh(e *FsmMsg) []*table.Path {
	m := e.MsgData.(*bgp.BGPMessage)
	rr := m.Body.(*bgp.BGPRouteRefresh)
	rf := bgp.AfiSafiToRouteFamily(rr.AFI, rr.SAFI)
	if _, ok := peer.fsm.rfMap[rf]; !ok {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.ID(),
			"Data":  rf,
		}).Warn("Route family isn't supported")
		return nil
	}
	if _, ok := peer.fsm.capMap[bgp.BGP_CAP_ROUTE_REFRESH]; !ok {
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.ID(),
		}).Warn("ROUTE_REFRESH received but the capability wasn't advertised")
		return nil
	}
	rfList := []bgp.RouteFamily{rf}
	peer.adjRibOut.Drop(rfList)
	accepted, filtered := peer.getBestFromLocal(rfList)
	peer.adjRibOut.Update(accepted)
	for _, path := range filtered {
		path.IsWithdraw = true
		accepted = append(accepted, path)
	}
	return accepted
}

func (peer *Peer) doPrefixLimit(k bgp.RouteFamily, c *config.PrefixLimitConfig) *bgp.BGPMessage {
	if maxPrefixes := int(c.MaxPrefixes); maxPrefixes > 0 {
		count := peer.adjRibIn.Count([]bgp.RouteFamily{k})
		pct := int(c.ShutdownThresholdPct)
		if pct > 0 && !peer.prefixLimitWarned[k] && count > (maxPrefixes*pct/100) {
			peer.prefixLimitWarned[k] = true
			log.WithFields(log.Fields{
				"Topic":         "Peer",
				"Key":           peer.ID(),
				"AddressFamily": k.String(),
			}).Warnf("prefix limit %d%% reached", pct)
		}
		if count > maxPrefixes {
			log.WithFields(log.Fields{
				"Topic":         "Peer",
				"Key":           peer.ID(),
				"AddressFamily": k.String(),
			}).Warnf("prefix limit reached")
			return bgp.NewBGPNotificationMessage(bgp.BGP_ERROR_CEASE, bgp.BGP_ERROR_SUB_MAXIMUM_NUMBER_OF_PREFIXES_REACHED, nil)
		}
	}
	return nil

}

func (peer *Peer) updatePrefixLimitConfig(c []config.AfiSafi) error {
	x := peer.fsm.pConf.AfiSafis
	y := c
	if len(x) != len(y) {
		return fmt.Errorf("changing supported afi-safi is not allowed")
	}
	m := make(map[bgp.RouteFamily]config.PrefixLimitConfig)
	for _, e := range x {
		k, err := bgp.GetRouteFamily(string(e.Config.AfiSafiName))
		if err != nil {
			return err
		}
		m[k] = e.PrefixLimit.Config
	}
	for _, e := range y {
		k, err := bgp.GetRouteFamily(string(e.Config.AfiSafiName))
		if err != nil {
			return err
		}
		if p, ok := m[k]; !ok {
			return fmt.Errorf("changing supported afi-safi is not allowed")
		} else if !p.Equal(&e.PrefixLimit.Config) {
			log.WithFields(log.Fields{
				"Topic":                   "Peer",
				"Key":                     peer.ID(),
				"AddressFamily":           e.Config.AfiSafiName,
				"OldMaxPrefixes":          p.MaxPrefixes,
				"NewMaxPrefixes":          e.PrefixLimit.Config.MaxPrefixes,
				"OldShutdownThresholdPct": p.ShutdownThresholdPct,
				"NewShutdownThresholdPct": e.PrefixLimit.Config.ShutdownThresholdPct,
			}).Warnf("update prefix limit configuration")
			peer.prefixLimitWarned[k] = false
			if msg := peer.doPrefixLimit(k, &e.PrefixLimit.Config); msg != nil {
				sendFsmOutgoingMsg(peer, nil, msg, true)
			}
		}
	}
	peer.fsm.pConf.AfiSafis = c
	return nil
}

func (peer *Peer) handleUpdate(e *FsmMsg) ([]*table.Path, []bgp.RouteFamily, *bgp.BGPMessage) {
	m := e.MsgData.(*bgp.BGPMessage)
	update := m.Body.(*bgp.BGPUpdate)
	log.WithFields(log.Fields{
		"Topic":       "Peer",
		"Key":         peer.fsm.pConf.Config.NeighborAddress,
		"nlri":        update.NLRI,
		"withdrawals": update.WithdrawnRoutes,
		"attributes":  update.PathAttributes,
	}).Debug("received update")
	peer.fsm.pConf.Timers.State.UpdateRecvTime = time.Now().Unix()
	if len(e.PathList) > 0 {
		peer.adjRibIn.Update(e.PathList)
		for _, family := range peer.fsm.pConf.AfiSafis {
			k, _ := bgp.GetRouteFamily(string(family.Config.AfiSafiName))
			if msg := peer.doPrefixLimit(k, &family.PrefixLimit.Config); msg != nil {
				return nil, nil, msg
			}
		}
		paths := make([]*table.Path, 0, len(e.PathList))
		eor := []bgp.RouteFamily{}
		for _, path := range e.PathList {
			if path.IsEOR() {
				family := path.GetRouteFamily()
				log.WithFields(log.Fields{
					"Topic":         "Peer",
					"Key":           peer.ID(),
					"AddressFamily": family,
				}).Debug("EOR received")
				eor = append(eor, family)
				continue
			}
			if path.Filtered(peer.ID()) != table.POLICY_DIRECTION_IN {
				paths = append(paths, path)
			}
		}
		return paths, eor, nil
	}
	return nil, nil, nil
}

func (peer *Peer) startFSMHandler(incoming *channels.InfiniteChannel, stateCh chan *FsmMsg) {
	peer.fsm.h = NewFSMHandler(peer.fsm, incoming, stateCh, peer.outgoing)
}

func (peer *Peer) StaleAll(rfList []bgp.RouteFamily) {
	peer.adjRibIn.StaleAll(rfList)
}

func (peer *Peer) PassConn(conn *net.TCPConn) {
	select {
	case peer.fsm.connCh <- conn:
	default:
		conn.Close()
		log.WithFields(log.Fields{
			"Topic": "Peer",
			"Key":   peer.ID(),
		}).Warn("accepted conn is closed to avoid be blocked")
	}
}

func (peer *Peer) ToConfig() *config.Neighbor {
	// create copy which can be access to without mutex
	conf := *peer.fsm.pConf

	conf.AfiSafis = make([]config.AfiSafi, len(peer.fsm.pConf.AfiSafis))
	for i := 0; i < len(peer.fsm.pConf.AfiSafis); i++ {
		conf.AfiSafis[i] = peer.fsm.pConf.AfiSafis[i]
	}

	remoteCap := make([][]byte, 0, len(peer.fsm.capMap))
	for _, c := range peer.fsm.capMap {
		for _, m := range c {
			buf, _ := m.Serialize()
			remoteCap = append(remoteCap, buf)
		}
	}
	conf.State.Capabilities.RemoteList = remoteCap

	caps := capabilitiesFromConfig(peer.fsm.pConf)
	localCap := make([][]byte, 0, len(caps))
	for _, c := range caps {
		buf, _ := c.Serialize()
		localCap = append(localCap, buf)
	}
	conf.State.Capabilities.LocalList = localCap

	conf.State.RemoteRouterId = peer.fsm.peerInfo.ID.To4().String()
	conf.State.SessionState = config.IntToSessionStateMap[int(peer.fsm.state)]
	conf.State.AdminState = peer.fsm.adminState.String()

	if peer.fsm.state == bgp.BGP_FSM_ESTABLISHED {
		rfList := peer.configuredRFlist()
		conf.State.AdjTable.Advertised = uint32(peer.adjRibOut.Count(rfList))
		conf.State.AdjTable.Received = uint32(peer.adjRibIn.Count(rfList))
		conf.State.AdjTable.Accepted = uint32(peer.adjRibIn.Accepted(rfList))

		conf.Transport.State.LocalAddress, conf.Transport.State.LocalPort = peer.fsm.LocalHostPort()
		_, conf.Transport.State.RemotePort = peer.fsm.RemoteHostPort()

		conf.State.ReceivedOpenMessage, _ = peer.fsm.recvOpen.Serialize()

	}
	return &conf
}

func (peer *Peer) DropAll(rfList []bgp.RouteFamily) {
	peer.adjRibIn.Drop(rfList)
	peer.adjRibOut.Drop(rfList)
}
