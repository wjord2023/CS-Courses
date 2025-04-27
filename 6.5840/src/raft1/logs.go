package raft

type Entries struct {
	Command any
	Term    int
	Index   int
}

type Logs []Entries

func (l Logs) LastIndex() int {
	return l[len(l)-1].Index
}

func (l Logs) LastTerm() int {
	return l[len(l)-1].Term
}

// get the relative index of the log
func (l Logs) RIndex(index int) int {
	return index - l[0].Index
}

func (l *Logs) New() {
	*l = []Entries{{Term: -1, Command: nil, Index: -1}}
}

func (l *Logs) Append(command any, term int) int {
	newIndex := (*l).LastIndex() + 1
	*l = append(*l, Entries{Command: command, Term: term, Index: newIndex})
	return newIndex
}
