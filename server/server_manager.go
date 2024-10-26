package server

type ServerManager interface {
	SpwanServer() RedisServer
}

type ServerManagerImpl struct {
	args    map[string]string
	master  *MasterServer
	replica *ReplicaServer
}

func NewServerManager(args map[string]string) ServerManager {
	return &ServerManagerImpl{args: args}
}

func (s *ServerManagerImpl) SpwanServer() RedisServer {
	if _, ok := s.args["--replicaof"]; ok {
		s.replica = NewReplicaServer(s.args)
		return s.replica
	}
	s.master = NewMasterServer(s.args)
	return s.master
}
