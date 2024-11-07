package server

// Design pattern: Factory Method
// This pattern is used to create Redis servers based on the arguments passed to the manager

const (
	MASTER  = "master"
	REPLICA = "replica"
)

// Manages the creation of Redis servers
type ServerManager interface {
	SpawnServer() RedisServer
}

// Implementation of the ServerManager interface
type ServerManagerImpl struct {
	args   map[string]string
	server RedisServer
}

func NewServerManager(args map[string]string) ServerManager {
	return &ServerManagerImpl{args: args}
}

// Spawns a Redis server based on the arguments passed to the manager
func (s *ServerManagerImpl) SpawnServer() RedisServer {
	switch getServerType(s.args) {
	case MASTER:
		s.server = NewMasterServer(s.args)
		return s.server
	case REPLICA:
		s.server = NewReplicaServer(s.args)
		return s.server
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
