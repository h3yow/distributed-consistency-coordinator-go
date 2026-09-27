package raft

import "sync"

// Handler 消息处理回调，由节点注册到网络。
type Handler func(msg *Message)

type linkKey struct{ a, b NodeID }

// Network 本地内存网络，模拟节点间通信，支持分区与断连。
// 默认全连通；Disconnect/Isolate 后对应方向的消息被丢弃。
type Network struct {
	mu       sync.Mutex
	handlers map[NodeID]Handler
	blocked  map[linkKey]bool // 单向阻塞：a->b 不可达
}

// NewNetwork 创建本地网络。
func NewNetwork() *Network {
	return &Network{
		handlers: make(map[NodeID]Handler),
		blocked:  make(map[linkKey]bool),
	}
}

// Register 注册节点消息处理器。
func (n *Network) Register(id NodeID, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[id] = h
}

// Unregister 注销节点（模拟进程崩溃：发给它的消息将被丢弃）。
func (n *Network) Unregister(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.handlers, id)
}

// Connect 恢复 a<->b 双向连通。
func (n *Network) Connect(a, b NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.blocked, linkKey{a, b})
	delete(n.blocked, linkKey{b, a})
}

// Disconnect 断开 a<->b 双向链路。
func (n *Network) Disconnect(a, b NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked[linkKey{a, b}] = true
	n.blocked[linkKey{b, a}] = true
}

// Isolate 将节点与所有其他节点隔离。
func (n *Network) Isolate(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for peer := range n.handlers {
		if peer == id {
			continue
		}
		n.blocked[linkKey{id, peer}] = true
		n.blocked[linkKey{peer, id}] = true
	}
}

// Heal 恢复全部连通。
func (n *Network) Heal() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.blocked = make(map[linkKey]bool)
}

// Send 异步投递消息；链路断开或对端未注册时丢弃。
func (n *Network) Send(msg *Message) {
	n.mu.Lock()
	if n.blocked[linkKey{msg.From, msg.To}] {
		n.mu.Unlock()
		return
	}
	h, ok := n.handlers[msg.To]
	n.mu.Unlock()
	if !ok {
		return // 对端已崩溃/未注册
	}
	// 异步投递，模拟真实网络且避免在处理链中死锁。
	go h(msg)
}
