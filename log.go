package gactor

import (
	"github.com/godyy/glog"
	"go.uber.org/zap"
)

// createStdLogger 创建面向标准输出的 logger.
func createStdLogger(level glog.Level) glog.Logger {
	return glog.NewLogger(&glog.Config{
		Level:        level,
		EnableCaller: true,
		CallerSkip:   0,
		Development:  true,
		Cores:        []glog.CoreConfig{glog.NewStdCoreConfig()},
	}).Named("gactor")
}

func lfdNodeId(nodeId string) zap.Field {
	return zap.String("node", nodeId)
}

func lfdRemoteNodeId(nodeId string) zap.Field {
	return zap.String("remoteNode", nodeId)
}

func lfdPacketType(pt PacketType) zap.Field {
	return zap.Int8("packetType", pt)
}

func lfdSeq(seq uint32) zap.Field {
	return zap.Uint32("seq", seq)
}

func lfdPacketTypeSeq(ph packetHead) zap.Field {
	return zap.Dict("packet",
		zap.Int8("type", ph.getPt()),
		zap.Uint32("seq", ph.getSeq()),
	)
}

func lfdRequestType(rt RequestType) zap.Field {
	return zap.String("requestType", rt.String())
}

func lfdReqId(reqId uint32) zap.Field {
	return zap.Uint32("reqId", reqId)
}

func lfdPanic(err any) zap.Field {
	return zap.Dict("panic",
		zap.Any("error", err),
		zap.StackSkip("stack", 3),
	)
}

func lfdSession(session ActorSession) zap.Field {
	return zap.Dict("session",
		zap.String("nodeId", session.NodeId),
		zap.Uint32("sid", session.SID),
	)
}

func lfdCategoryName(name string) zap.Field {
	return zap.String("category", name)
}

func lfdId(id int64) zap.Field {
	return zap.Int64("id", id)
}

func lfdError(err error) zap.Field {
	return zap.NamedError("error", err)
}

func lfdSid(sid uint32) zap.Field {
	return zap.Uint32("sid", sid)
}

func lfdTimeout(timeout int64) zap.Field {
	return zap.Int64("timeout", timeout)
}

func lfdErrCode(ec errCode) zap.Field {
	return zap.String("errcode", ec.String())
}

func (s *Service) lfdActorUID(name string, uid ActorUID) zap.Field {
	if ad := s.actorDefineSet.getDefine(uid.Category); ad != nil {
		return zap.Dict(
			name,
			zap.String("category", ad.common().Name),
			zap.Int64("id", uid.ID),
		)
	} else {
		return zap.Dict(
			name,
			zap.Uint16("category", uid.Category),
			zap.Int64("id", uid.ID),
		)
	}
}
