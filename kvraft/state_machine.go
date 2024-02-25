package kvraft

type MemoryKVStateMachine struct {
	KV map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}

func (mkv *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := mkv.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (mkv *MemoryKVStateMachine) Put(key, value string) Err {
	mkv.KV[key] = value
	return OK
}

func (mkv *MemoryKVStateMachine) Append(key, value string) Err {
	mkv.KV[key] += value
	return OK
}
