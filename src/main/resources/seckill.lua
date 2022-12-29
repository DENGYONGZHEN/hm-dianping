--参数列表
--1.1 优惠券的id，用户id
local voucherId = ARGV[1]
local userId = ARGV[2]
--订单id
local orderId = ARGV[3]

--数据key
--1.库存key，订单key
local stockKey = 'seckill:stock:'..voucherId
local orderKey = 'seckill:order:'..voucherId
--lua脚本业务
if(tonumber(redis.call('get',stockKey))<=0)then
    return 1
end
--判断用户是否下单
if(redis.call('sismember',orderKey, userId) == 1)then
--存在说明重复下单，返回2，不允许再次下单
    return 2
end
--减库存操作
redis.call('incrby',stockKey,-1)
--保存订单
redis.call('sadd',orderKey, userId)
--发送消息到队列中  xadd stream.order * k1 v1 k2 v2
redis.call('xadd','stream.order','*','userId',userId,'voucherId',voucherId,'id',orderId)
return 0