using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using cAlgo.API;

namespace cAlgo.Robots
{
    [Robot(TimeZone = TimeZones.UTC, AccessRights = AccessRights.None)]
    public class MainBot : Robot
    {
        // --- ⚙️ 参数配置 ---
        [Parameter("运行模式", DefaultValue = RunningMode.DataCollection)]
        public RunningMode CurrentMode { get; set; }

        [Parameter("批次大小 (Batch)", DefaultValue = 1000)]
        public int BatchSize { get; set; }

        public enum RunningMode { DataCollection, LiveTrading }

        // --- 🛠️ 模块化成员 ---
        private NetworkClient _net;
        private readonly DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private List<object[]> _dataBuffer = new List<object[]>();

        // 传输统计
        private long _totalSentItems = 0; // 累计已成功发送的数据点数量
        private long _totalSentBytes = 0; // 累计已发送字节数

        protected override void OnStart()
        {
            Print("=== TradeBrain Client v1.1 (流式架构) 启动 ===");
            
            _net = new NetworkClient("127.0.0.1", 8888);
            if (!_net.Connect())
            {
                Print("❌ [Network] 无法连接服务器，请检查 Python 端是否开启");
                Stop();
                return;
            }
            Print("✅ [Network] 服务器连接成功");
        }

        protected override void OnTick()
        {
            // 核心逻辑：依靠回测引擎推进，不再使用任何 while 循环
            CollectAndProcess();
        }

        private void CollectAndProcess()
        {
            // 1. 数据采样 (当前 Tick)
            double unixTs = (Server.Time - _epoch).TotalSeconds;
            double midPrice = (Symbol.Bid + Symbol.Ask) / 2;
            
            // 2. 根据模式执行逻辑
            if (CurrentMode == RunningMode.DataCollection)
            {
                _dataBuffer.Add(new object[] { unixTs, midPrice });
                
                // 只有缓冲区满时才触发 Socket 传输，分摊开销
                if (_dataBuffer.Count >= BatchSize)
                {
                    FlushDataBuffer();
                }
            }
            else if (CurrentMode == RunningMode.LiveTrading)
            {
                // TODO: 插入预测与绘图逻辑
                RequestPrediction(midPrice);
            }
        }

        private void FlushDataBuffer()
        {
            var payload = new { type = "FEED_DATA", data = _dataBuffer };
            string json = JsonSerializer.Serialize(payload); // 高效序列化

            int sentCount = _dataBuffer.Count;
            int byteCount = Encoding.UTF8.GetByteCount(json);

            string response = _net.SendAndReceive(json);

            if (response != null && response.Contains("saved"))
            {
                // 更新统计
                _totalSentItems += sentCount;
                _totalSentBytes += byteCount;

                Print($"📥 [Pipeline] 成功传输 {sentCount} 条数据 | 本次字节: {byteCount} | 当前时间: {Server.Time}");
                Print($"📊 [Stats] 累计已传输: {_totalSentItems} 条, {_totalSentBytes} 字节");
            }
            else
            {
                Print("⚠️ [Network] 传输确认失败或超时");
            }

            _dataBuffer.Clear();
        }

        private void RequestPrediction(double price)
        {
            // 同步请求预测，利用 OnTick 的频率确保不卡顿
            string json = JsonSerializer.Serialize(new { type = "PREDICT", price = price });
            string response = _net.SendAndReceive(json);
            
            // 此处后续接入 Visualizer 模块进行绘图
        }

        protected override void OnStop()
        {
            _net?.Close();
            Print("=== 机器人停止，资源已释放 ===");
        }
    }
}