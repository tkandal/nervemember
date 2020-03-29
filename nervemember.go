package nervemember

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/tkandal/zookclient"
)

// NerveMember is an instance of Nerve-status in Zookeeper.
// Nerve is used in AirBnB's Nerve. https://github.com/airbnb/nerve
type NerveMember struct {
	Host string `json:"host"`
	Port int    `json:"port"`
	Name string `json:"name"`
}

// Nerve is an struct for maintaining Nerve-members in Zookeeper
type Nerve struct {
	host        string
	zkClient    *zookclient.ZooKeeperClient
	path        string
	nerveMember *NerveMember
}

// NewNerve connects to the given host and create a Nerve-member in the given path, if
// an error occurs, returns nil and an error
func NewNerve(host string, path string, nm *NerveMember) (*Nerve, error) {
	nerve := &Nerve{
		host:        host,
		path:        path,
		nerveMember: nm,
	}
	zkClient, err := zookclient.NewZooKeeperClient(host)
	if err != nil {
		return nil, fmt.Errorf("connect to %s failed; error = %v", nerve.host, err)
	}
	nerve.zkClient = zkClient

	content, err := toBytes(nm)
	if err != nil {
		return nil, fmt.Errorf("encode nerve-member %s:%s failed; error = %v", nerve.host, path, err)
	}
	if err = nerve.zkClient.CreateEphemeralNode(path, content); err != nil {
		return nil, fmt.Errorf("create node %s:%s failed; error = %v", nerve.host, path, err)
	}
	return nerve, nil
}

// ReadNerveMember returns the Nerve-member if it exists, otherwise nil and an error
func (n *Nerve) ReadNerveMember() (*NerveMember, error) {
	if !n.zkClient.Exists(n.path) {
		return nil, fmt.Errorf("path %s:%s does not exist", n.host, n.path)
	}
	content, err := n.zkClient.GetData(n.path)
	if err != nil {
		return nil, fmt.Errorf("get data from %s:%s failed; error = %v", n.host, n.path, err)
	}
	if content == nil || len(content) == 0 {
		return nil, fmt.Errorf("%s:%s does not contain any data", n.host, n.path)
	}
	buf := bytes.NewBuffer(content)
	nm := &NerveMember{}
	if err := json.NewDecoder(buf).Decode(nm); err != nil {
		return nil, fmt.Errorf("decode nerve-member %s:%s failed; error = %v", n.host, n.path, err)
	}
	return nm, nil
}

// Close closes the connection to Zookeeper and removes the Nerve-member.
func (n *Nerve) Close() error {
	return n.zkClient.Close()
}

func toBytes(obj interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0))
	if err := json.NewEncoder(buf).Encode(obj); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
