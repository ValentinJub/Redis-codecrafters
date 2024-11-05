package server

const (
	MASTER  = "master"
	REPLICA = "replica"
)

// Manages the creation of Redis servers
type ServerManager interface {
	SpwanServer() RedisServer
}

// Implementation of the ServerManager interface
type ServerManagerImpl struct {
	args    map[string]string
	master  MasterServer
	replica ReplicaServer
}

func NewServerManager(args map[string]string) ServerManager {
	return &ServerManagerImpl{args: args}
}

// Spawns a Redis server based on the arguments passed to the manager
func (s *ServerManagerImpl) SpwanServer() RedisServer {
	switch getServerType(s.args) {
	case MASTER:
		s.master = NewMasterServer(s.args)
		return s.master
	case REPLICA:
		s.replica = NewReplicaServer(s.args)
		return s.replica
	default:
		return nil
	}
}

// Parses args and return the server type
func getServerType(args map[string]string) string {
	if _, ok := args["--replicaof"]; ok {
		return REPLICA
	}
	return MASTER
}
