package tokenRegexQuery

type Transition struct {
	node *ParseNfaNode
	edge *ParseNfaEdge
}

type ParseNfaNode struct {
	nexts []*Transition
	prevs []*Transition
}

type ParseNfaEdge struct {
	label    string
	epsilon  bool
	wildcard bool
	checked  bool
	start    *ParseNfaNode
	end      *ParseNfaNode
}

type Nfa struct {
	nodes []*ParseNfaNode
	edges []*ParseNfaEdge
	inode *ParseNfaNode
	fnode *ParseNfaNode
}

type SubNfa struct {
	start *ParseNfaNode
	end   *ParseNfaNode
}

func NewTransition(node *ParseNfaNode, edge *ParseNfaEdge) *Transition {
	return &Transition{
		node: node,
		edge: edge,
	}
}

func NewSubNfa(start *ParseNfaNode, end *ParseNfaNode) *SubNfa {
	return &SubNfa{
		start: start,
		end:   end,
	}
}

func NewParseNfaNode() *ParseNfaNode {
	return &ParseNfaNode{
		nexts: make([]*Transition, 0),
		prevs: make([]*Transition, 0),
	}
}

func NewParseNfaEdge(label string, start *ParseNfaNode, end *ParseNfaNode) *ParseNfaEdge {
	// the checked should be set to false in order to check wildcard
	edge := ParseNfaEdge{label, false, false, false, start, end}
	if label == "" {
		edge.epsilon = true
	} else if label == "." {
		edge.wildcard = true
	}
	return &edge
}

func NewNfa() *Nfa {
	return &Nfa{
		nodes: make([]*ParseNfaNode, 0),
		edges: make([]*ParseNfaEdge, 0),
		inode: nil,
		fnode: nil,
	}
}

/*
	generate NFA using syntax tree
*/
func GenerateNfa(root *ParseTreeNode) *Nfa {
	nfa := NewNfa()
	subnfa := nfa.BuildNFA(root)
	nfa.inode = subnfa.start
	nfa.fnode = subnfa.end
	//nfa.CheckWildcard(nfa.inode)
	return nfa
}

/*
	generate subNFA with different operator
*/
func (n *Nfa) BuildNFA(node *ParseTreeNode) *SubNfa {
	if node.isoperator {
		if node.value == "*" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", sub.end, sub.start)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", start, end)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "+" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", sub.end, sub.start)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "&" {
			subl := n.BuildNFA(node.lchild)
			subr := n.BuildNFA(node.rchild)
			n.AddEdge("", subl.end, subr.start)
			return NewSubNfa(subl.start, subr.end)
		} else if node.value == "|" {
			subl := n.BuildNFA(node.lchild)
			subr := n.BuildNFA(node.rchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", start, subl.start)
			n.AddEdge("", start, subr.start)
			n.AddEdge("", subl.end, end)
			n.AddEdge("", subr.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "?" {
			sub := n.BuildNFA(node.lchild)
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge("", start, sub.start)
			n.AddEdge("", start, end)
			n.AddEdge("", sub.end, end)
			return NewSubNfa(start, end)
		} else if node.value == "." {
			start := NewParseNfaNode()
			end := NewParseNfaNode()
			n.AddNode(start)
			n.AddNode(end)
			n.AddEdge(".", start, end)
			return NewSubNfa(start, end)
		} else {
			return nil
			// other operator
		}
	} else {
		return n.BuildNfaWithString(node.value)
	}
}

/*
	build NFA through string
*/
func (n *Nfa) BuildNfaWithString(str string) *SubNfa {
	start := NewParseNfaNode()
	start_ := start
	n.nodes = append(n.nodes, start)
	for i := 0; i < len(str); i++ {
		end := NewParseNfaNode()
		n.AddNode(end)
		n.AddEdge(str[i:i+1], start, end)
		start = end
	}
	return NewSubNfa(start_, start)

}

/*
	add edge into NFA
*/
func (n *Nfa) AddEdge(label string, start *ParseNfaNode, end *ParseNfaNode) {
	edge := NewParseNfaEdge(label, start, end)
	start.nexts = append(start.nexts, NewTransition(end, edge))
	n.edges = append(n.edges, edge)
	prevsEdge := NewParseNfaEdge(label, end, start)
	end.prevs = append(end.prevs, NewTransition(start, prevsEdge))
	n.edges = append(n.edges, prevsEdge)
}

/*
	add node into NFA
*/
func (n *Nfa) AddNode(node *ParseNfaNode) {
	n.nodes = append(n.nodes, node)
}

func (n *Nfa) getPrefix(length int) map[string]struct{} {
	startNode := n.inode
	result := make(map[string]struct{}, 0)
	startNode.getPrefixRecursion(length, result, "")
	return result
}

func (node *ParseNfaNode) getPrefixRecursion(length int, result map[string]struct{}, prefix string) {
	if len(node.nexts) == 0 {
		result[prefix] = struct{}{}
		return
	}
	if len(prefix) == length {
		result[prefix] = struct{}{}
		return
	}
	for i := 0; i < len(node.nexts); i++ {
		if node.nexts[i].edge.label == "." {
			result[prefix] = struct{}{}
			continue
		}
		node.nexts[i].node.getPrefixRecursion(length, result, prefix+node.nexts[i].edge.label)
	}
}

func (n *Nfa) getSuffix(length int) map[string]struct{} {
	startNode := n.fnode
	result := make(map[string]struct{}, 0)
	startNode.getSuffixRecursion(length, result, "")
	reversalResult := make(map[string]struct{}, 0)
	for k, _ := range result {
		reversalResult[reverseString(k)] = struct{}{}
	}
	return reversalResult
}

func (node *ParseNfaNode) getSuffixRecursion(length int, result map[string]struct{}, suffix string) {
	if len(node.prevs) == 0 {
		result[suffix] = struct{}{}
		return
	}
	if len(suffix) == length {
		result[suffix] = struct{}{}
		return
	}
	for i := 0; i < len(node.prevs); i++ {
		if node.prevs[i].edge.label == "." {
			result[suffix] = struct{}{}
			continue
		}
		node.prevs[i].node.getSuffixRecursion(length, result, suffix+node.prevs[i].edge.label)
	}
}
