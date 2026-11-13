<p align="center">
  <img src="client/web/logo.png" width="110" alt="HCMUT Logo" />
</p>

# Assignment 1: Distributed Streaming-based Collaboration Application
> **Trường Đại Học Bách Khoa – ĐHQG TP.HCM (HCMUT)**  
> **Khoa Khoa học & Kỹ thuật Máy tính (CSE)**  
> **Môn học:** CO3093 – Mạng Máy Tính (HK261) | **Nhóm 02 – Lớp L01**  
> • Ngô Tuấn Khôi – 2411697 | • Đặng Phúc Khang – 2411421 | • Huỳnh Tiến Dũng – 2410587  

---

## 1. Kiến trúc Hệ thống

Ứng dụng cộng tác Client-Server và P2P thời gian thực được xây dựng **100% bằng Socket thuần mức thấp (`import socket`)**, không sử dụng framework mạng bậc cao (Flask/Django) hay WebSockets.

```text
                     ┌──────────────────────────────────────────────┐
                     │          Central Server (Port 8888)          │
                     │ Auth · Global Chat Hub · E2EE Relay · Watchdog│
                     └──────────────────────┬───────────────────────┘
                                ▲           │           ▲
                TCP Signaling   │           │           │   TCP Signaling
                & Global Chat   │           │           │   & Global Chat
                                ▼           │           ▼
            ┌────────────────────────┐       │       ┌────────────────────────┐
            │      Client: Alice     │       │       │       Client: Bob      │
            │    TCP / UDP Sockets   │       │       │    TCP / UDP Sockets   │
            │  AES-GCM · Web: 5001   │       │       │  AES-GCM · Web: 5002   │
            └───────────┬────────────┘       │       └────────────┬───────────┘
                        │                    │                    │
           REST Polling │         P2P UDP Data Stream             │ REST Polling
                        │  ◄───────────────────────────────────►  │
                        │     (Direct Streaming / AES-256-GCM)   │
                        ▼                                         ▼
            ┌────────────────────────┐               ┌────────────────────────┐
            │     Browser: Alice     │               │      Browser: Bob      │
            │  http://127.0.0.1:5001 │               │  http://127.0.0.1:5002 │
            └────────────────────────┘               └────────────────────────┘
```

* **TCP Signaling (Port 8888):** Bắt tay 2 chiều, xác thực/đăng ký với mật khẩu **`scrypt + salt`** (RFC 7914), cấp phát **Auth Token**, chuyển tiếp tin nhắn Chat E2EE (Broadcast/Unicast) và Watchdog timeout 15s.
* **UDP Data Streaming (Port 8889 / Dynamic P2P):** Truyền tệp tin cậy bằng **Selective Repeat ARQ** (cửa sổ trượt $N=8$, chunk $\le 1200\text{B}$ chống HoL blocking), đóng/hủy luồng qua cờ **`TYPE_FIN`**, nhịp tim và đồng bộ danh bạ qua UDP có xác thực Token.
* **Bảo mật Tầng Ứng dụng:** Mã hóa xác thực **AES-256-GCM AEAD** với nonce 12-byte ngẫu nhiên.
* **Web UI Socket thuần (Zero-Framework HTTP/1.1):** Dashboard trực quan, REST polling cập nhật tin nhắn và thanh tiến trình $0\% \to 100\%$.

---

## 2. Cài đặt Môi trường

Yêu cầu: **Python 3.10+** (Windows, macOS hoặc Linux).
```bash
pip install -r requirements.txt
```
*(Chỉ sử dụng thư viện `cryptography` cho thuật toán chuẩn AES-256-GCM).*

---

## 3. Hướng dẫn Khởi chạy Hệ thống

Mở các cửa sổ Terminal riêng biệt:

### Bước 1: Khởi động Central Server
```bash
python -m server.run_server
```
*(Server lắng nghe tại `0.0.0.0:8888` cho TCP và `8889` cho UDP).*

### Bước 2: Khởi động Client Alice (Web UI cổng 5001)
```bash
python -m client.run_client Alice --ui-port 5001
```

### Bước 3: Khởi động Client Bob (Web UI cổng 5002)
```bash
python -m client.run_client Bob --ui-port 5002
```

> **Chạy giữa 2 máy tính khác nhau trong mạng LAN / Wi-Fi:**  
> Trên máy 2 (Client Bob), thêm tham số `--host <IP_Máy_1>`:  
> `python -m client.run_client Bob --host 192.168.1.15`

---

## 4. Kiểm thử Tự Động (Test Suite)

Chạy bộ 18 unit tests tự động:
```bash
python -m unittest discover tests -v
```

---

## 5. Cấu trúc Thư mục Dự án

```text
assignment1/
├── Report_BTL1.pdf            # Báo cáo kỹ thuật thuyết minh đồ án (Technical Report PDF)
├── protocol/                  # Tầng giao thức & Thuật toán truyền thông
│   ├── packet.py              # Đóng khung TCP nhị phân 20B, MsgType, CRC32
│   ├── crypto.py              # Mã hóa AES-256-GCM, băm mật khẩu scrypt + salt
│   └── udp_stream.py          # Selective Repeat ARQ (Window N=8), TYPE_FIN, UDP Heartbeat
├── server/                    # Tầng Server trung tâm (Signaling Plane)
│   ├── server.py              # Server đa luồng TCP/UDP, Watchdog quét timeout
│   ├── user_manager.py        # Quản lý session, danh bạ online, tài khoản, token
│   └── run_server.py          # Module thực thi Server
├── client/                    # Tầng Client ứng dụng
│   ├── client.py              # Client Network Engine (Dual-plane TCP/UDP)
│   ├── web_server.py          # Server HTTP/1.1 socket thuần (Zero-Framework Web UI)
│   ├── run_client.py          # Module thực thi Client
│   ├── downloads/             # Thư mục lưu tệp nhận qua P2P UDP
│   └── web/                   # Giao diện Web (index.html, style.css, app.js, logo.png)
├── tests/                     # Bộ kiểm thử tự động (18 unit tests)
├── requirements.txt           # Danh mục thư viện phụ thuộc
├── .gitignore                 # Cấu hình bỏ qua tệp tạm, cache và tệp tải về
└── README.md                  # Hướng dẫn kỹ thuật và vận hành
```
