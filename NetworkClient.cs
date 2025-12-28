using System;
using System.Net.Sockets;
using System.Text;

namespace cAlgo.Robots
{
    public class NetworkClient : IDisposable
    {
        private TcpClient _client;
        private NetworkStream _stream;
        private readonly string _host;
        private readonly int _port;

        public NetworkClient(string host, int port)
        {
            _host = host;
            _port = port;
        }

        public bool Connect()
        {
            try {
                _client = new TcpClient(_host, _port);
                _stream = _client.GetStream();
                return true;
            } catch { return false; }
        }

        public string SendAndReceive(string message)
        {
            if (_stream == null || !_client.Connected) return null;
            try {
                // 设置读写超时（5秒）以避免无限阻塞
                _stream.ReadTimeout = 5000;
                _stream.WriteTimeout = 5000;
                
                byte[] data = Encoding.UTF8.GetBytes(message + "\n");
                _stream.Write(data, 0, data.Length);

                // 循环读取直到接收到完整响应
                byte[] buffer = new byte[8192];
                int totalBytesRead = 0;
                int bytesRead = 0;
                
                while (totalBytesRead < buffer.Length)
                {
                    bytesRead = _stream.Read(buffer, totalBytesRead, buffer.Length - totalBytesRead);
                    if (bytesRead == 0) break;  // 连接关闭
                    totalBytesRead += bytesRead;
                    
                    // 如果收到换行符，认为消息完整
                    if (totalBytesRead > 0 && buffer[totalBytesRead - 1] == '\n')
                        break;
                }
                
                return totalBytesRead > 0 ? Encoding.UTF8.GetString(buffer, 0, totalBytesRead) : null;
            } catch { return null; }
        }

        public void Close() { 
            try { _stream?.Close(); } catch {};
            try { _client?.Close(); } catch {};
        }

        public void Dispose()
        {
            try { _stream?.Dispose(); } catch {};
            try { _client?.Dispose(); } catch {};
        }
    }
}