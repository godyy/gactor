package gactor

import (
	"github.com/godyy/gutils/log"
	"go.uber.org/zap"
)

// createStdLogger 创建面向标准输出的 logger.
func createStdLogger(level log.Level) log.Logger {
	 return log.NewLogger(&log.Config{
		Level:        level,
		EnableCaller: true,
		CallerSkip:   0,
		Development:  true,
		Cores:        []log.CoreConfig{log.NewStdCoreConfig()},
	}).Named("gactor")
}

func lfdNodeId(nodeId string) zap.Field {
	return zap.String("nodeId", nodeId)
}

func lfdPacketType(pt PacketType) zap.Field {
	return zap.Int8("packetType", pt)
}

func lfdRequestType(rt RequestType) zap.Field {
	return zap.String("requestType", rt.String())
}

func lfdActor(category string, id int64) zap.Field {
	return zap.Dict("actor",
		zap.String("category", category),
		zap.Int64("id", id),
	)
}

func lfdActorWithImpl(impl actorImpl) zap.Field {
	return lfdActor(impl.core().Name, impl.ActorUID().ID)
}

func lfdActorUID(uid ActorUID) zap.Field {
	return zap.Dict("actorUID",
		zap.Uint16("category", uid.Category),
		zap.Int64("id", uid.ID),
	)
}

func lfdReqId(reqId uint64) zap.Field {
	return zap.Uint64("reqId", reqId)
}

func lfdPanic(err any) zap.Field {
	return zap.Dict("panic",
		zap.Any("error", err),
		zap.StackSkip("stack", 1),
	)
}

func lfdSession(session ActorSession) zap.Field {
	return zap.Dict("session",
		zap.String("nodeId", session.NodeId),
		zap.Uint32("sid", session.SID),
	)
}

func lfdCategory(category uint16) zap.Field {
	return zap.Uint16("category", category)
}

func lfdId(id int64) zap.Field {
	return zap.Int64("id", id)
}

func lfdError(err error) zap.Field {
	return zap.NamedError("error", err)
}

func (s *Service) lfdActor(uid ActorUID) zap.Field {
	ad := s.actorDefineSet.getDefine(uid.Category)
	return lfdActor(ad.common().Name, uid.ID)
}
