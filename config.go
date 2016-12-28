package groupcache

import (
	"time"
	"strings"
	"errors"
	"fmt"
	"strconv"

	"github.com/samuel/go-zookeeper/zk"
	log "github.com/Sirupsen/logrus" // todo: use log provider
)

const (
	GroupCacheZNode = "groupcache"
	GroupCacheNodesZNode = GroupCacheZNode + "/nodes"
)

var (
	zkFlagPermanent = int32(0)
	defaultACL = zk.WorldACL(zk.PermAll)
)

type ConfigOpts struct {
	ZkServers      string

	ParentZNode    string

	SessionTimeout time.Duration
}

type ConfigWatcher struct {
	conn        *zk.Conn
	parentZNode string
}

func NewConfigWatcher(zkServers string, parentZNode string, sessionTimeout time.Duration) (w *ConfigWatcher, err error) {
	servers := strings.Split(zkServers, ",")
	log.Infof("connecting to servers: %v", servers)
	conn, _, err := zk.Connect(servers, sessionTimeout) // Note: sessionTimeout will be overwrite by TimeOut of server connectResponse when authenticating.
	if err != nil {
		return
	}
	if strings.HasSuffix(parentZNode, "/") {
		err = errors.New("invalid znode: " + parentZNode)
		return
	}
	w = &ConfigWatcher{conn: conn, parentZNode: parentZNode}
	return
}

func joinZNode(nodes... string) string {
	return strings.Join(nodes, "/")
}

// watchNodeList watches children of GroupCacheNodesZkNode and run callback process
func (w *ConfigWatcher) watchNodeList(process func(map[string]*[]byte) error) error {
	path := joinZNode(w.parentZNode, GroupCacheNodesZNode)

	// get children and watch,
	// then run callback process
	watch := func() (<-chan zk.Event, error) {
		nodes, _, ech, err := w.conn.ChildrenW(path)
		if err != nil {
			return nil, err
		}

		nodeConfig := make(map[string]*[]byte)
		for _, node := range nodes {
			nodePath := joinZNode(path, node)
			log.Infoln("getting path:", nodePath)
			data, _, err := w.conn.Get(nodePath)
			if err != nil {
				log.Errorln("getting path data failed, error:", err)
				continue
			}
			nodeConfig[node] = &data
		}

		err = process(nodeConfig)
		if err != nil {
			log.Errorln("processing nodelist falied, error :", err)
			return ech, err
		}

		return ech, nil
	}

	log.Infoln("watching path:", path)
	var ech <-chan zk.Event
	var err error
	if ech, err = watch(); err != nil {
		log.Errorf("watching path: %s failed.", path)
		return err
	}

	go func() {
		for {
			evt := <-ech
			log.Infof("node list changed, evt: %+v, watching path: %s", evt, path)
			if ech, err = watch(); err != nil {
				log.Errorf("watching path: %s failed.", path)
			}
		}
	}()

	return nil
}

func (w *ConfigWatcher) Close() {
	w.conn.Close()
}

func createZNodeIfNotExist(conn *zk.Conn, path string, data []byte, flags int32, acl []zk.ACL) error {
	if exists, _, err := conn.Exists(path); err != nil {
		log.Errorln("failed to check existence:", path)
		return err
	} else if !exists {
		_, err = conn.Create(path, data, flags, acl)
		if err != nil {
			log.Errorln("failed to create path:", path)
			if err != errors.New("zk: node already exists") {
				return err
			}
		}
	}
	return nil
}

func checkAddress(addr string) bool {
	parts := strings.Split(addr, ":")
	if len(parts) != 2 {
		return false
	}

	if _, err := strconv.Atoi(parts[1]); err != nil {
		return false
	}

	portions := strings.Split(parts[0], ".")
	if len(portions) != 4 {
		return false
	}

	for _, portion := range portions {
		if num, err := strconv.Atoi(portion); err != nil || !(num >= 0 && num <= 255) {
			return false
		}
	}

	return true
}

// registerSelf registers self as child of GroupCacheNodesZkNode, parentZNode should be
// existed in zookeeper beforehand.
func (p *HTTPPool) registerSelf() error {
	if p.self == "" || !strings.HasPrefix(p.self, "http://") {
		return errors.New("invalid self address")
	}

	selfAddr := p.self[strings.Index(p.self, "//") + 2:]
	if !checkAddress(selfAddr) {
		return errors.New("invalid self address")
	}

	path := joinZNode(p.configWatcher.parentZNode, GroupCacheZNode)
	log.Infoln("creating path if not exists. path:", path)
	if err := createZNodeIfNotExist(p.configWatcher.conn, path, nil, zkFlagPermanent, defaultACL); err != nil {
		return err
	}

	path = joinZNode(p.configWatcher.parentZNode, GroupCacheNodesZNode)
	log.Infoln("creating path if not exists. path:", path)
	if err := createZNodeIfNotExist(p.configWatcher.conn, path, nil, zkFlagPermanent, defaultACL); err != nil {
		return err
	}

	path = joinZNode(p.configWatcher.parentZNode, GroupCacheNodesZNode, selfAddr)
	log.Infoln("checking path:", path)
	for tries := 0; ; tries++ {
		if tries >= 5 {
			return errors.New(fmt.Sprintf("failed to checking existence of path: %s", path))
		}

		if exists, _, err := p.configWatcher.conn.Exists(path); err != nil {
			log.Infof("checking existence failed, err: %v", err)
		} else if exists {
			log.Infof("%s already exist, sleeping...", path)
			time.Sleep(p.opts.SessionTimeout * time.Duration(tries))
		} else {
			break
		}
	}

	log.Infoln("creating path:", path)
	_, err := p.configWatcher.conn.Create(path, nil, zk.FlagEphemeral, defaultACL)
	return err
}

func contains(arr []string, t string) bool {
	for _, s := range arr {
		if s == t {
			return true
		}
	}
	return false
}

func (p *HTTPPool) watchConfig() error {
	return p.configWatcher.watchNodeList(func(nodeConfig map[string]*[]byte) (err error) {
		nodes := make([]string, len(nodeConfig))
		i := 0
		for k := range nodeConfig {
			nodes[i] = k
			i++
		}

		nodeSelf := p.self[strings.Index(p.self, "//") + 2:]
		if !contains(nodes, nodeSelf) {
			log.Warnln("Node list not contains self, adding self to node list. node list: %v, self: %s", nodes, nodeSelf)
			// Add self to avoid cyclic loop of peer requests
			nodes = append(nodes, nodeSelf)
		}

		var urls []string
		for _, addr := range nodes {
			if !checkAddress(addr) {
				log.Errorf("invalid address: %s", addr)
				continue
			}
			urls = append(urls, fmt.Sprintf("http://%s", addr))
		}

		log.Infof("updating pool, urls: %v", urls)
		p.Set(urls...)
		return
	})
}

func (p *HTTPPool) RegisterAndWatch() (err error) {
	if err = p.registerSelf() ; err != nil {
		return
	}
	if err = p.watchConfig() ; err != nil {
		return
	}
	return
}

func (p *HTTPPool) HashRingKeys() []int {
	return p.peers.Keys()
}

type noWatchImplementation struct {}
func (noWatchImplementation) RegisterAndWatch() error {
	return errors.New("no implementation")
}