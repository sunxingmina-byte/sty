import base64
import json
import time
import logging
import os
import hashlib
import hmac
import yaml
from typing import List, Dict, Any, Tuple
from urllib.parse import urlencode
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============ 日志与基础配置 ============
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 从环境变量读取敏感信息，避免硬编码
UID = "f33e3c8d-6d03-40f4-963c-6cfd753e15d6"
HMAC_KEY = b'5f5749e77a9b'
VERSION = '2.3.0'
PLATFORM = 'win'
MAX_WORKERS = 10  # 适当减小线程数，避免触发 API 频控

if not UID or not HMAC_KEY:
    logger.error("环境变量 MY_UID 或 MY_HMAC_KEY 未设置！")
    exit(1)

class SubscriptionManager:
    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.raw_nodes = []
        self.clash_yaml = ""
        self.singbox_json = ""
        self.v2ray_base64 = ""

    def get_node_config(self, host: str) -> Dict:
        timestamp_ms = int(time.time() * 1000)
        body_data = {'timestamp': timestamp_ms, 'uid': UID, 'version': VERSION}
        
        signature = base64.b64encode(
            hmac.new(HMAC_KEY, urlencode(body_data).encode('utf-8'), hashlib.sha256).digest()
        ).decode('utf-8')

        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 GreenHub/2.3.0",
            "content-type": "application/json"
        }
        params = {'uid': UID, 'version': VERSION, 'platform': PLATFORM, 'sign': signature}
        
        try:
            response = self.session.post(f"https://{host}/d/v1/account", json=body_data, params=params, headers=headers, timeout=10)
            return response.json()
        except Exception as e:
            logger.debug(f"节点 {host} 请求失败: {e}")
            return {}

    def _process_single_node(self, idx: int, srv_label: str, node: Dict[str, Any]) -> Tuple[int, Dict]:
        session_result = self.get_node_config(node["domain"])
        data = session_result.get("data", {})
        if not data.get("port") or not data.get("id"):
            return None
        return idx, {
            "name": f"[{srv_label}]{node['label_zh']}",
            "server": node["domain"],
            "port": int(data["port"]),
            "uuid": data["id"],
            "path": data.get("path", "/")
        }

    def refresh_nodes(self):
        logger.info("开始刷新节点...")
        res = self.session.get("https://d1fumiloozdotj.cloudfront.net/v1/server_list_v7.json", timeout=10)
        server_list = res.json().get("data", [])
        
        tasks = []
        idx = 0
        for srv in server_list:
            if srv["id"] == "QQJD": continue
            for node in srv.get("servers", []):
                tasks.append((idx, srv["label_zh"], node))
                idx += 1

        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(self._process_single_node, *t) for t in tasks]
            for f in as_completed(futures):
                if f.result(): results.append(f.result())
        
        results.sort(key=lambda x: x[0])
        self.raw_nodes = [r[1] for r in results]
        
        if self.raw_nodes:
            self.build_clash_config()
            self.build_singbox_config()
            self.build_v2ray_sub()
            logger.info(f"生成完毕，共 {len(self.raw_nodes)} 个节点。")
        else:
            logger.error("未获取到有效节点。")

    def build_clash_config(self):
        proxies = [{"name": n["name"], "type": "vmess", "server": n["server"], "port": n["port"], 
                    "uuid": n["uuid"], "alterId": 0, "cipher": "auto", "tls": True, 
                    "network": "ws", "ws-opts": {"path": n["path"]}} for n in self.raw_nodes]
        names = [n["name"] for n in self.raw_nodes]
        config = {
            "proxies": proxies,
            "proxy-groups": [
                {"name": "Auto", "type": "url-test", "url": "http://www.gstatic.com/generate_204", "proxies": names},
                {"name": "Manual", "type": "select", "proxies": names}
            ],
            "rules": ["GEOSITE,cn,DIRECT", "GEOIP,CN,DIRECT", "MATCH,Auto"]
        }
        self.clash_yaml = yaml.dump(config, allow_unicode=True)

    def build_singbox_config(self):
        tags = [n["name"] for n in self.raw_nodes]
        outbounds = [
            {"type": "urltest", "tag": "AUTO", "outbounds": tags},
            {"type": "selector", "tag": "MANUAL", "outbounds": tags}
        ] + [{"type": "vmess", "tag": n["name"], "server": n["server"], "server_port": n["port"], 
              "uuid": n["uuid"], "transport": {"type": "ws", "path": n["path"]}} for n in self.raw_nodes]
        self.singbox_json = json.dumps({"outbounds": outbounds}, indent=2, ensure_ascii=False)

    def build_v2ray_sub(self):
        links = []
        for n in self.raw_nodes:
            v = {"v": "2", "ps": n["name"], "add": n["server"], "port": str(n["port"]), "id": n["uuid"], "net": "ws", "path": n["path"], "tls": "tls"}
            links.append("vmess://" + base64.b64encode(json.dumps(v).encode()).decode())
        self.v2ray_base64 = base64.b64encode("\n".join(links).encode()).decode()

if __name__ == "__main__":
    os.makedirs("dist", exist_ok=True)
    sm = SubscriptionManager()
    sm.refresh_nodes()
    if sm.raw_nodes:
        for name, content in [("clash.yaml", sm.clash_yaml), ("singbox.json", sm.singbox_json), ("v2ray.txt", sm.v2ray_base64)]:
            with open(f"dist/{name}", "w", encoding="utf-8") as f: f.write(content)
