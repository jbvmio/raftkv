package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spf13/pflag"
)

// Command line defaults
const (
	DefaultAppName  = `rkrr`
	DefaultHTTPAddr = ":11000"
	DefaultRaftAddr = ":12000"
)

// Command line parameters
var inmem bool
var httpAddr string
var raftAddr string
var joinAddr string
var peerList string
var peerArray []string
var nodeID string
var pf *pflag.FlagSet

func init() {
	pf = pflag.NewFlagSet(DefaultAppName, pflag.ExitOnError)
	pf.BoolVar(&inmem, "inmem", false, "Use in-memory storage for Raft")
	pf.StringVar(&httpAddr, "haddr", DefaultHTTPAddr, "Set the HTTP bind address")
	pf.StringVar(&raftAddr, "raddr", DefaultRaftAddr, "Set Raft bind address")
	pf.StringVar(&joinAddr, "join", "", "Set join address, if any, use if joining an existing cluster")
	pf.StringVar(&peerList, "peers", "", "Comma separated list of peers, use instead of join")
	pf.StringVar(&nodeID, "id", "", "Node ID")
}

func main() {
	pf.Parse(os.Args[1:])
	args := pf.Args()
	var raftDir string
	switch {
	case inmem:
	case len(args) > 1:
		fmt.Fprintf(os.Stderr, "Too many arguments for storage dir\n")
		os.Exit(1)
	case len(args) == 1:
		raftDir = args[0]
		os.MkdirAll(raftDir, 0700)
	default:
		inmem = true
	}

	s := NewRaftStore(inmem)
	s.RaftDir = raftDir
	s.RaftBind = raftAddr
	if err := s.Open(joinAddr == "", nodeID); err != nil {
		log.Fatalf("failed to open store: %s", err.Error())
	}

	h := NewHTTP(httpAddr, s)
	if err := h.Start(); err != nil {
		log.Fatalf("failed to start HTTP service: %s", err.Error())
	}

	switch {
	case peerList != "":
		var backoff int
		peerArray, backoff = parsePeerList(httpAddr, peerList)
		switch backoff {
		case 0:
		default:
			time.Sleep(time.Second * time.Duration(backoff*2))
			var successful bool
			for !successful {
				for _, p := range peerArray {
					if p != httpAddr {
						if err := join(p, raftAddr, nodeID); err != nil {
							log.Printf("Joining %v failed, need at least 1 more peer", p)
						} else {
							joinAddr = p
							//sendKV(p, raftAddr, nodeID)
							successful = true
							break
						}
						time.Sleep(time.Second * 3)
					}
				}
			}
		}
	case joinAddr != "":
		if err := join(joinAddr, raftAddr, nodeID); err != nil {
			log.Fatalf("failed to join node at %s: %s", joinAddr, err.Error())
		}
		//sendKV(joinAddr, raftAddr, nodeID)
	}

	log.Println(DefaultAppName, "started successfully")

	stopChan := make(chan struct{})
	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		timer := time.NewTicker(time.Second * 5)
	workLoop:
		for {
			select {
			case <-stopChan:
				break workLoop
			case <-timer.C:
				switch {
				case s.Raft().State().String() == `Leader`:
					fmt.Printf("I AM LEADER (%v)\n", raftAddr)
					s.Set(`LEADER`, raftAddr)
					pl := s.MatchRegex(`^RKRRPEER_`)
					switch len(pl) {
					default:
						fmt.Println("PEERS:")
						for _, p := range pl {
							fmt.Println(" ", p)
						}
					}
				case s.Raft().State().String() == `Follower`:
					fmt.Printf("I AM FOLLOWER (%v)\n", raftAddr)
					lastContact := time.Now().Unix() - s.Raft().LastContact().Unix()
					l, _ := s.Get(`LEADER`)
					fmt.Printf("LEADER %v last contacted %v seconds ago\n", l, lastContact)
					sendKV(joinAddr, `RKRRPEER_`+nodeID, raftAddr+`_`+nodeID)
				}
			}
		}
	}()

	<-terminate
	close(stopChan)
	wg.Wait()
	log.Println(DefaultAppName, "exiting")
}

func join(joinAddr, raftAddr, nodeID string) error {
	b, err := json.Marshal(map[string]string{"addr": raftAddr, "id": nodeID})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/join", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func sendKV(joinAddr, key, val string) error {
	b, err := json.Marshal(map[string]string{key: val})
	if err != nil {
		return err
	}
	resp, err := http.Post(fmt.Sprintf("http://%s/key", joinAddr), "application-type/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}

func parsePeerList(me, peerList string) (peers []string, backoff int) {
	backoff = -1
	peers = strings.Split(peerList, `,`)
Reconcile:
	switch len(peers) {
	case 0:
		backoff = 0
	case 1:
		switch {
		case peers[0] == me:
			backoff = 0
			return
		default:
			peers = append(peers, me)
		}
	default:
		for _, p := range peers {
			if p == me {
				break Reconcile
			}
		}
		peers = append(peers, me)
	}
	sort.SliceStable(peers, func(i, j int) bool {
		return peers[i] < peers[j]
	})
	for i := 0; i < len(peers); i++ {
		if peers[i] == me {
			backoff = i
			break
		}
	}
	return
}
