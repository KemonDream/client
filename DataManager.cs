using System;
using System.Collections.Generic;
using System.Text.Json;
using cAlgo.API;

namespace cAlgo.Robots
{
    public class DataManager
    {
        private readonly Robot _bot;
        private readonly DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // 实时采集批处理相关
        private List<object[]> _currentBatch = new List<object[]>();
        private readonly object _batchLock = new object();
        private const int BatchSize = 1000;

        // 分块回溯相关
        private DateTime _currentSeekTime;

        public DataManager(Robot bot) 
        { 
            _bot = bot; 
            _currentSeekTime = _bot.Server.Time;
        }

        // 每当 OnTick 触发时调用此方法
        public void CollectTick(Action<string> sendAction)
        {
            if (sendAction == null) return;

            try
            {
                double unixTs = (_bot.Server.Time - _epoch).TotalSeconds;
                double price = (_bot.Symbol.Bid + _bot.Symbol.Ask) / 2;

                List<object[]> batchToSend = null;

                lock (_batchLock)
                {
                    _currentBatch.Add(new object[] { unixTs, price });
                    if (_currentBatch.Count >= BatchSize)
                    {
                        batchToSend = new List<object[]>(_currentBatch);
                        _currentBatch.Clear();
                    }
                }

                if (batchToSend != null)
                {
                    var payload = new { type = "FEED_DATA", data = batchToSend };
                    string json = System.Text.Json.JsonSerializer.Serialize(payload);

                    try
                    {
                        sendAction(json);
                        _bot.Print($"📥 数据包已发出: {batchToSend.Count} 条 (当前时间: {_bot.Server.Time})");
                    }
                    catch (Exception ex)
                    {
                        _bot.Print($"❌ 发送数据失败，已回滚批次: {ex.Message}");
                        lock (_batchLock)
                        {
                            _currentBatch.InsertRange(0, batchToSend);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _bot.Print($"❌ CollectTick 错误: {ex.Message}");
            }
        }

        // 在停止或需要时手动刷新剩余数据
        public void Flush(Action<string> sendAction)
        {
            if (sendAction == null) return;

            List<object[]> batchToSend = null;
            lock (_batchLock)
            {
                if (_currentBatch.Count == 0) return;
                batchToSend = new List<object[]>(_currentBatch);
                _currentBatch.Clear();
            }

            try
            {
                var payload = new { type = "FEED_DATA", data = batchToSend };
                string json = System.Text.Json.JsonSerializer.Serialize(payload);
                sendAction(json);
                _bot.Print($"📤 Flush 已发送: {batchToSend.Count} 条");
            }
            catch (Exception ex)
            {
                _bot.Print($"❌ Flush 发送失败: {ex.Message}");
                lock (_batchLock)
                {
                    _currentBatch.InsertRange(0, batchToSend);
                }
            }
        }

        public List<string> GetHistoricalTicks(int monthsBack)
        {
            DateTime endTime = _bot.Server.Time;
            DateTime startTime = endTime.AddMonths(-monthsBack);
            List<string> data = new List<string>();

            try
            {
                // 获取 Tick 序列（最高精度的数据源）
                var ticks = _bot.MarketData.GetTicks(_bot.SymbolName);

                // 强制回溯加载至指定时间
                _bot.Print($"⏳ 开始回溯加载至: {startTime}...");
                while (ticks.Count == 0 || ticks[0].Time > startTime)
                {
                    int loaded = ticks.LoadMoreHistory();
                    if (loaded == 0) break;  // 已经没有更多数据
                }

                // 遍历 Tick 数据并筛选时间范围
                for (int i = 0; i < ticks.Count; i++)
                {
                    var t = ticks[i];
                    if (t.Time < startTime) continue;
                    if (t.Time > endTime) break;

                    double unixTs = (t.Time - _epoch).TotalSeconds;
                    // 使用 Bid 价格（与 Python 端格式一致）
                    data.Add($"[{unixTs:F3}, {t.Bid}]");
                }

                _bot.Print($"✅ 历史数据加载完成，总计: {data.Count} 条。");
            }
            catch (Exception ex)
            {
                _bot.Print($"❌ 获取历史数据失败: {ex.Message}");
            }

            return data;
        }

        

        // 分块回溯数据，防止单次执行时间过长导致 UI 假死
        public bool StreamDataChunk(DateTime targetStartTime, Action<List<string>> sendAction)
        {
            DateTime chunkEnd = _currentSeekTime;
            DateTime chunkStart = _currentSeekTime.AddDays(-1);

            if (chunkStart < targetStartTime) chunkStart = targetStartTime;

            // 使用 GetTicks(symbol) 并确保回溯加载到需要的开始时间
            var ticks = _bot.MarketData.GetTicks(_bot.SymbolName);
            while (ticks.Count == 0 || ticks[0].Time > chunkStart)
            {
                int loaded = ticks.LoadMoreHistory();
                if (loaded == 0) break;
            }

            // 筛选当前片段时间范围内的 ticks
            List<string> data = new List<string>();
            for (int i = 0; i < ticks.Count; i++)
            {
                var tk = ticks[i];
                if (tk.Time < chunkStart) continue;
                if (tk.Time > chunkEnd) break;
                double unixTs = (tk.Time - _epoch).TotalSeconds;
                data.Add($"[{unixTs:F3}, {tk.Bid}]");
            }

            if (data.Count > 0) sendAction(data);

            _currentSeekTime = chunkStart;
            return _currentSeekTime > targetStartTime; // 返回是否还需要继续挖
        }
    }
}
