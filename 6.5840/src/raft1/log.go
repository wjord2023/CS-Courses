package raft

type Entries struct {
	Command any
	Term    int
	Index   int
}

type Log []Entries

func (l Log) LastIndex() int {
	return l[len(l)-1].Index
}

func (l Log) LastTerm() int {
	return l[len(l)-1].Term
}

func (l Log) FirstIndex() int {
	return l[0].Index
}

func (l Log) FirstTerm() int {
	return l[0].Term
}

func (l Log) Get(index int) Entries {
	return l[index-l.FirstIndex()]
}

func (l Log) GetSlice(start int, end int) []Entries {
	return l[start-l.FirstIndex() : end-l.FirstIndex()]
}

func (l *Log) Init() {
	*l = make([]Entries, 0)
	*l = append(*l, Entries{Command: nil, Term: 0, Index: 0})
}

func (l *Log) Append(command any, term int) int {
	index := l.LastIndex() + 1
	*l = append(*l, Entries{Command: command, Term: term, Index: index})
	return index
}
