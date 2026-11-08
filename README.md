<p align="center">
  <img src="logo.png" width="120" alt="Trường ĐH Bách Khoa - ĐHQG TP.HCM Logo" />
</p>

# NexusStream: Distributed Streaming Collaboration System
> **Course:** CO3093 – Phát triển Ứng dụng Mạng (Network Application Development)  
> **Assignment 1:** Distributed and Streaming-based Client-Server Collaboration Application  

---

## 1. Giới thiệu Tổng quan

**NexusStream** là ứng dụng cộng tác thời gian thực phân tán được xây dựng 100% bằng socket nhị phân mức thấp (`import socket`): 

- **Kiến trúc phân tách 2 kênh (Dual-Plane Architecture):**
  - **TCP Signaling Plane:** Bắt tay 2 chiều, xác thực đăng nhập, quản lý phiên kết nối, phát hiện người dùng (Peer Discovery), gửi nhận tin nhắn chat thời gian thực và tín hiệu báo trước khi truyền file.
  - **UDP Data Streaming Plane:** Truyền file tin cậy bằng giải thuật **Selective Repeat ARQ** (Sliding Window $N=8$, chunk 1200 bytes $< \text{MTU } 1500\text{B}$), loại bỏ hoàn toàn hiện tượng nghẽn đầu hàng (**Head-of-Line Blocking**).
- **Bảo mật Tầng Ứng dụng (E2EE):** Mã hóa xác thực **AES-256-GCM** (AEAD) với IV ngẫu nhiên 12B cho cả tin nhắn và từng chunk dữ liệu UDP (Server chỉ đóng vai trò Zero-Knowledge Relay).
- **Web UI thuần Socket:** Tự xây dựng máy chủ HTTP/1.1 bằng socket TCP thô phục vụ giao diện Web hiện đại, cập nhật danh bạ và tiến trình truyền file thời gian thực.

---

## 2. Bảng Đối chiếu 10 Tiêu chí Đề bài (Rubric 10/10 Điểm)

| Tiêu chí (Milestone) | Yêu cầu theo Rubric | Hiện thực trong Mã nguồn | Điểm |
| :--- | :--- | :--- | :---: |
| **Step 1: Setup & Protocol Design** | Header nhị phân, cờ Flags, CRC32, Framing. | [`protocol/packet.py`](protocol/packet.py): Header TCP 20B cố định `!HBBB3sIII`, hàm `recv_exact` chống dính/xé gói. | 1 đ |
| **Step 2: Socket Setup & Handshake** | Bắt tay 2 chiều TCP giữa Client và Server. | [`server/server.py`](server/server.py) & [`client/client.py`](client/client.py): Cặp gói tin `HANDSHAKE_REQ` và `HANDSHAKE_ACK`. | 1 đ |
| **Step 3: Auth & State Management** | Đăng nhập tên duy nhất, quản lý phiên, lưu IP/Port. | [`server/user_manager.py`](server/user_manager.py): Lưu `UserSession` an toàn đa luồng (`RLock`), broadcast `USER_JOINED`. | 1 đ |
| **Step 4: Concurrency Implementation** | Đa luồng (Multi-threading), hỗ trợ 10–20 clients. | Mô hình Thread-per-Client, kiểm thử thành công 15 clients đồng thời trong [`tests/test_integration.py`](tests/test_integration.py). | 1 đ |
| **Step 5: Time-sensitive Streaming** | Định tuyến chat chung (Broadcast) và riêng (Unicast). | Gói tin `MSG_BROADCAST`, `MSG_UNICAST`, kèm gói xác nhận `MSG_DELIVERY_ACK`. | 1 đ |
| **Step 6: Reliable File Transfer** | Truyền file UDP tin cậy, cắt chunk, đối soát toàn vẹn. | [`protocol/udp_stream.py`](protocol/udp_stream.py): Selective Repeat ARQ, cửa sổ trượt $N=8$, chunk 1200B, mã hash SHA-256. | 1 đ |
| **Step 7: Resilience & Error Handling** | Chịu lỗi sập nguồn, timeout socket, không crash server. | Bắt lỗi `ConnectionResetError`, `BrokenPipeError`; Watchdog quét định kỳ 5s đá user im lặng > 15s. | 1 đ |
| **Step 8: Security Layer** | Mã hóa dữ liệu E2EE trước khi gửi và giải mã khi nhận. | [`protocol/crypto.py`](protocol/crypto.py): Thuật toán chuẩn **AES-256-GCM** (khóa 32B, IV 12B), chống Replay Attack. | 1 đ |
| **Step 9: HTTP-based UI with Client JS** | Giao diện Web hiển thị chat, tiến trình tải, danh bạ online. | [`client/web_server.py`](client/web_server.py): Server HTTP/1.1 socket thuần phục vụ giao diện Web Dashboard, không dùng framework. | 1 đ |
| **Step 10: Testing & Code Quality** | Kiểm thử tự động hóa toàn diện, code sạch sẽ. | 3 bộ test tự động (`tests/`): kiểm tra giao thức nhị phân, UDP streaming và kiểm thử tích hợp 15 clients. | 1 đ |

---

## 3. Cấu trúc Thư mục Dự án

```text
assignment1/
├── protocol/                           # [Tầng Giao thức Ứng dụng]
│   ├── packet.py                      # Header TCP nhị phân 20B, CRC32, recv_exact chống dính/xé gói
│   ├── crypto.py                      # Mã hóa đối xứng AES-256-GCM (khóa 32B, IV 12B)
│   └── udp_stream.py                  # Selective Repeat ARQ, Sliding Window N=8, chunk 1200B
├── server/                             # [Tầng Máy chủ Trung tâm]
│   ├── server.py                      # TCP Server đa luồng non-blocking, Watchdog định kỳ 5s
│   ├── user_manager.py                # Quản lý danh bạ online, kiểm soát phiên an toàn luồng (RLock)
│   └── run_server.py                  # Điểm khởi chạy Server dòng lệnh
├── client/                             # [Tầng Máy khách Client]
│   ├── client.py                      # Động cơ Dual-Plane (TCP Signaling + UDP Transfer)
│   ├── web_server.py                  # Server HTTP/1.1 bằng Socket thuần (REST API /api/*)
│   ├── run_client.py                  # Khởi chạy Client (hỗ trợ cả Web UI lẫn Terminal CLI)
│   ├── downloads/                     # Thư mục lưu tệp nhận qua UDP
│   └── web/                           # Giao diện Web Client (HTML5, Modern CSS, Vanilla JS)
├── tests/                              # [Bộ Kiểm thử Tự động hóa Toàn diện]
│   ├── test_protocol.py               # Kiểm tra Header 20B, CRC32, mã hóa AES-GCM
│   ├── test_udp_stream.py             # Kiểm tra Selective Repeat ARQ và toàn vẹn SHA-256
│   └── test_integration.py            # Kiểm tra 15 clients đồng thời, ngắt socket đột ngột RST
├── run_demo.py                         # 1 lệnh tự động bật Server + 2 Client và mở trình duyệt
└── README.md                          # Hướng dẫn chi tiết vận hành và đối chiếu Rubric
```

---

## 4. Yêu cầu Hệ thống & Cài đặt

- **Môi trường:** Python 3.10 trở lên (Windows, macOS, Linux).
- **Cài đặt thư viện:** Dự án chỉ sử dụng duy nhất thư viện mật mã chuẩn:
  ```bash
  pip install cryptography
  ```

---

## 5. Hướng dẫn Vận hành Hệ thống

### Cách 1: Khởi chạy Nhanh Tự Động (Khuyên dùng)
Chạy một lệnh duy nhất để tự động bật Server và mở sẵn 2 Client (Alice & Bob) trên trình duyệt:
```bash
python run_demo.py
```
- Alice: `http://127.0.0.1:5001`
- Bob: `http://127.0.0.1:5002`  
*(Bấm `Ctrl + C` tại Terminal để dừng toàn bộ hệ thống).*

---

### Cách 2: Khởi chạy Thủ công Từng Tiến trình

1. **Khởi động Server trung tâm (Terminal 1):**
   ```bash
   python -m server.run_server
   ```
   *(Server lắng nghe trên `0.0.0.0` với TCP port `8888` và UDP port `8889`).*

2. **Khởi động các Client (Mở các Terminal khác):**
   - **Chế độ Web UI (Mặc định):**
     ```bash
     python -m client.run_client Alice
     python -m client.run_client Bob
     ```
     *(Hệ thống tự động cấp phát cổng Web UI 5001, 5002... và tự mở trình duyệt).*
   - **Chế độ Dòng lệnh (CLI Mode):**
     ```bash
     python -m client.run_client Charlie --cli
     ```
     *Lệnh CLI: `/send <text>`, `/msg <user> <text>`, `/file <user> <path>`, `/peers`, `/status`, `/quit`.*

---

### Cách 3: Demo trên 2 Laptop Khác Nhau (Chung mạng Wi-Fi LAN)
1. Trên **Laptop 1** (chạy Server): Gõ `ipconfig` lấy địa chỉ IPv4 (ví dụ `192.168.1.15`), sau đó chạy:
   ```bash
   python -m server.run_server
   python -m client.run_client Alice
   ```
2. Trên **Laptop 2** (Client Bob): Kết nối tới IP của Laptop 1:
   ```bash
   python -m client.run_client Bob --host 192.168.1.15
   ```
   *(Hoặc mở Web UI trên Laptop 2, bấm **Connect**, nhập Server IP là `192.168.1.15` rồi bấm **Log In`).*

---

## 6. Chạy Kiểm thử Tự động (Automated Test Suite)

Dự án cung cấp 3 bộ test độc lập đánh giá 100% các tiêu chí kỹ thuật:

```bash
# 1. Kiểm thử đóng gói Header 20B, CRC32 và mã hóa AES-256-GCM (Step 1 & 8)
python -m unittest tests/test_protocol.py

# 2. Kiểm thử truyền file Selective Repeat ARQ và đối soát SHA-256 (Step 6)
python -m unittest tests/test_udp_stream.py

# 3. Kiểm thử tải đồng thời 15 clients và khả năng chịu lỗi sập nguồn (Step 2, 3, 4, 5, 7)
python tests/test_integration.py
```
