package server

type ServerManager interface {
	SpwanServer() RedisServer
}

type ServerManagerImpl struct {
	args map[string]string
}

func NewServerManager(args map[string]string) ServerManager {
	return &ServerManagerImpl{args: args}
}

func (s *ServerManagerImpl) SpwanServer() RedisServer {
	if _, ok := s.args["--replicaof"]; ok {
		return NewReplicaServer(s.args)
	}
	return NewMasterServer(s.args)
}
