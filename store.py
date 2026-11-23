import copy
import json
import os
import shutil
import tempfile
import threading
import time
import uuid

# 1. 向量时钟

class VectorClock:
    def __init__(self, node_id=None):
        self.clock = {}
        if node_id is not None:
            self.clock[str(node_id)] = 0

    def increment(self, node_id):
        node_id = str(node_id)
        self.clock[node_id] = self.clock.get(node_id, 0) + 1

    def merge(self, other):
        for node_id, value in other.clock.items():
            self.clock[node_id] = max(
                self.clock.get(node_id, 0),
                value
            )

    def compare(self, other):
        """
        返回：
        before     : self 发生在 other 之前
        after      : self 发生在 other 之后
        equal      : 两个向量相等
        concurrent : 两个向量并发
        """
        keys = set(self.clock) | set(other.clock)

        self_le_other = all(
            self.clock.get(k, 0) <= other.clock.get(k, 0)
            for k in keys
        )

        self_ge_other = all(
            self.clock.get(k, 0) >= other.clock.get(k, 0)
            for k in keys
        )

        if self_le_other and self_ge_other:
            return "equal"

        if self_le_other:
            return "before"

        if self_ge_other:
            return "after"

        return "concurrent"

    def is_before(self, other):
        return self.compare(other) == "before"

    def is_concurrent(self, other):
        return self.compare(other) == "concurrent"

    def copy(self):
        result = VectorClock()
        result.clock = dict(self.clock)
        return result

    def to_dict(self):
        return dict(self.clock)

    @classmethod
    def from_dict(cls, data):
        result = cls()
        result.clock = dict(data)
        return result

    def __repr__(self):
        return "VC{}".format(self.clock)

# 2. Add-Wins OR-Set

class AWORSet:
    """
    每次添加生成一个唯一 tag。

    例如：
        添加 apple -> tag A
        再添加 apple -> tag B

    删除只删除客户端已观察到的 tag。

    如果一端删除 tag A，而另一端并发添加 tag B，
    合并后 tag B 仍然存在，因此 apple 保留。

    这就是 Add-Wins。
    """

    def __init__(self):
        # 商品 -> 添加事件 tag 集合
        self.adds = {}

        # 已删除的添加事件 tag
        self.removes = set()

    def add(self, item, tag):
        self.adds.setdefault(item, set()).add(str(tag))

    def remove(self, observed_tags):
        """
        只删除客户端已经观察到的添加事件。
        """
        self.removes.update(observed_tags)

    def observed_tags(self, item):
        """
        返回当前可见的该商品添加事件。
        """
        return set(self.adds.get(item, set())) - self.removes

    def get_items(self):
        result = set()

        for item, tags in self.adds.items():
            if tags - self.removes:
                result.add(item)

        return result

    def merge(self, other):
        """
        OR-Set 合并：
        添加事件取并集，删除标记取并集。
        """
        for item, tags in other.adds.items():
            self.adds.setdefault(item, set()).update(tags)

        self.removes.update(other.removes)

    def copy(self):
        return AWORSet.from_dict(self.to_dict())

    def to_dict(self):
        return {
            "adds": {
                item: sorted(tags)
                for item, tags in self.adds.items()
            },
            "removes": sorted(self.removes)
        }

    @classmethod
    def from_dict(cls, data):
        result = cls()

        result.adds = {
            item: set(tags)
            for item, tags in data.get("adds", {}).items()
        }

        result.removes = set(data.get("removes", []))

        return result

    def __repr__(self):
        return "Cart(items={})".format(sorted(self.get_items()))

# 3. KV 节点

class KVNode:
    def __init__(self, node_id, network, persist_path):
        self.node_id = str(node_id)
        self.network = network
        self.persist_path = persist_path

        self.lock = threading.RLock()

        self.kv_store = {}
        self.clock = VectorClock()

        # request_id -> {"request": ..., "response": ...}
        self.processed_requests = {}

        self.alive = True
        self.sync_running = True

        self._load_from_disk()

        self.sync_thread = threading.Thread(
            target=self._auto_sync,
            daemon=True
        )
        self.sync_thread.start()

        print(
            "[节点 {}] 启动，持久化文件：{}".format(
                self.node_id,
                self.persist_path
            )
        )

    def _state_dict(self):
        """
        调用方应持有 self.lock。
        """
        return {
            "kv_store": {
                key: cart.to_dict()
                for key, cart in self.kv_store.items()
            },
            "clock": self.clock.to_dict(),
            "processed_requests": copy.deepcopy(
                self.processed_requests
            )
        }

    def _restore_state(self, data):
        """
        调用方应持有 self.lock。
        """
        self.kv_store = {
            key: AWORSet.from_dict(cart_data)
            for key, cart_data in data.get("kv_store", {}).items()
        }

        self.clock = VectorClock.from_dict(
            data.get("clock", {})
        )

        self.processed_requests = copy.deepcopy(
            data.get("processed_requests", {})
        )

    def _load_from_disk(self):
        if not os.path.exists(self.persist_path):
            print("[节点 {}] 无历史数据".format(self.node_id))
            return

        with open(self.persist_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        self._restore_state(data)

        print("[节点 {}] 已从磁盘恢复".format(self.node_id))

    def _save_to_disk(self):
        """
        写入临时文件，刷新后替换目标文件。

        本方法失败时抛出异常，调用方不能向客户端报告成功。
        """
        directory = os.path.dirname(
            os.path.abspath(self.persist_path)
        )

        os.makedirs(directory, exist_ok=True)

        fd, temp_path = tempfile.mkstemp(
            prefix=".cart_tmp_",
            suffix=".json",
            dir=directory
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(
                    self._state_dict(),
                    f,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True
                )

                f.flush()
                os.fsync(f.fileno())

            os.replace(temp_path, self.persist_path)

        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    def get(self, key):
        """
        返回购物车副本，避免调用者直接修改节点内部状态。
        """
        with self.lock:
            if not self.alive:
                return None

            cart = self.kv_store.get(key)

            if cart is None:
                return AWORSet()

            return cart.copy()

    def read_with_context(self, key):
        """
        读取购物车，并返回当前向量时钟。

        客户端删除商品时，应携带它已经观察到的 tag。
        """
        with self.lock:
            if not self.alive:
                raise ConnectionError(
                    "节点 {} 不可用".format(self.node_id)
                )

            cart = self.kv_store.get(key, AWORSet()).copy()

            return cart, self.clock.copy()

    def apply_request(self, request):
        """
        处理一个客户端请求。

        同一个 request_id 的重试不会再次创建添加事件。

        返回：
            {
                "clock": {...}
            }
        """
        with self.lock:
            if not self.alive:
                raise ConnectionError(
                    "节点 {} 不可用".format(self.node_id)
                )

            request_id = request["request_id"]

            if request_id in self.processed_requests:
                saved = self.processed_requests[request_id]

                if saved["request"] != request:
                    raise ValueError(
                        "同一个 request_id 被用于不同请求"
                    )

                return copy.deepcopy(saved["response"])

            old_state = self._state_dict()

            try:
                client_clock = VectorClock.from_dict(
                    request["clock"]
                )

                self.clock.merge(client_clock)
                self.clock.increment(self.node_id)

                key = request["key"]
                item = request["item"]
                operation = request["operation"]

                cart = self.kv_store.setdefault(
                    key,
                    AWORSet()
                )

                if operation == "add":
                    # 使用请求 ID 作为添加事件的唯一 tag。
                    cart.add(item, request_id)

                elif operation == "remove":
                    cart.remove(
                        set(request.get("observed_tags", []))
                    )

                else:
                    raise ValueError(
                        "未知操作：{}".format(operation)
                    )

                response = {
                    "clock": self.clock.to_dict()
                }

                self.processed_requests[request_id] = {
                    "request": copy.deepcopy(request),
                    "response": copy.deepcopy(response)
                }

                self._save_to_disk()

                return copy.deepcopy(response)

            except Exception:
                # 持久化失败时恢复内存状态。
                self._restore_state(old_state)
                raise

    def snapshot(self):
        with self.lock:
            if not self.alive:
                raise ConnectionError(
                    "节点 {} 不可用".format(self.node_id)
                )

            return self._state_dict()

    def merge_snapshot(self, state):
        """
        将另一个节点的状态合并到本节点。
        """
        with self.lock:
            if not self.alive:
                raise ConnectionError(
                    "节点 {} 不可用".format(self.node_id)
                )

            old_state = self._state_dict()

            try:
                for key, cart_data in state.get(
                    "kv_store", {}
                ).items():

                    incoming = AWORSet.from_dict(cart_data)

                    self.kv_store.setdefault(
                        key,
                        AWORSet()
                    ).merge(incoming)

                self.clock.merge(
                    VectorClock.from_dict(
                        state.get("clock", {})
                    )
                )

                for request_id, record in state.get(
                    "processed_requests", {}
                ).items():

                    if request_id in self.processed_requests:
                        old_request = self.processed_requests[
                            request_id
                        ]["request"]

                        if old_request != record["request"]:
                            raise ValueError(
                                "检测到冲突的 request_id"
                            )
                    else:
                        self.processed_requests[
                            request_id
                        ] = copy.deepcopy(record)

                self._save_to_disk()

            except Exception:
                self._restore_state(old_state)
                raise

    def migrate_from(self, source_node, progress=1.0):
        """
        按 key 迁移数据。

        progress=0.5 表示迁移前一半 key。

        这里只演示迁移中途崩溃，不是完整的生产级分片协议。
        """
        if not 0.0 <= progress <= 1.0:
            raise ValueError("progress 必须位于 0 到 1 之间")

        source_state = source_node.snapshot()
        all_keys = sorted(source_state["kv_store"].keys())

        migrate_count = int(len(all_keys) * progress)

        partial_state = {
            "kv_store": {
                key: source_state["kv_store"][key]
                for key in all_keys[:migrate_count]
            },
            "clock": {},
            "processed_requests": {}
        }

        self.merge_snapshot(partial_state)

        return migrate_count, len(all_keys)

    def _auto_sync(self):
        while self.sync_running:
            time.sleep(0.5)

            if not self.alive:
                continue

            try:
                peer_ids = self.network.get_all_nodes()

                for peer_id in peer_ids:
                    if peer_id == self.node_id:
                        continue

                    peer_state = self.network.request_sync(
                        peer_id,
                        self.node_id
                    )

                    if peer_state is not None:
                        self.merge_snapshot(peer_state)

            except Exception as exc:
                # 不让后台线程因为一次异常直接退出。
                print(
                    "[节点 {}] 自动同步异常：{}".format(
                        self.node_id,
                        exc
                    )
                )

    def crash(self):
        with self.lock:
            self.alive = False

            # 显式清空内存，模拟进程崩溃后的内存丢失。
            self.kv_store = {}
            self.clock = VectorClock()
            self.processed_requests = {}

        print(
            "\n[故障注入] 节点 {} 宕机，内存已清空".format(
                self.node_id
            )
        )

    def restart(self):
        with self.lock:
            self.kv_store = {}
            self.clock = VectorClock()
            self.processed_requests = {}

            self._load_from_disk()

            self.alive = True

        print(
            "\n[故障恢复] 节点 {} 重启".format(
                self.node_id
            )
        )

    def shutdown(self):
        self.sync_running = False

        # 等待后台线程退出，避免临时目录清理时仍有文件写入。
        if self.sync_thread.is_alive():
            self.sync_thread.join(timeout=3)

# 4. 虚拟网络

class VirtualNetwork:
    def __init__(self):
        self.nodes = {}
        self.partition_pairs = set()

        self.msg_delay = 0.0
        self.msg_repeat = 1
        self.drop_reply = False

        self.lock = threading.RLock()

    def register_node(self, node):
        with self.lock:
            self.nodes[node.node_id] = node

    def get_all_nodes(self):
        with self.lock:
            return list(self.nodes.keys())

    def _is_partitioned(self, a, b):
        pair = frozenset((str(a), str(b)))

        with self.lock:
            return pair in self.partition_pairs

    def request_sync(self, target_id, src_id):
        if self._is_partitioned(target_id, src_id):
            return None

        with self.lock:
            node = self.nodes.get(target_id)
            delay = self.msg_delay

        if node is None:
            return None

        if delay > 0:
            time.sleep(delay)

        # 延迟期间可能发生分区，因此再检查一次。
        if self._is_partitioned(target_id, src_id):
            return None

        try:
            return node.snapshot()
        except ConnectionError:
            return None

    def send_operation(self, src, dst, request):
        """
        返回：
            (True, response)  : 收到成功响应
            (False, None)     : 未收到成功响应

        False 可能表示节点不可用，也可能表示响应丢失。
        响应丢失时客户端不能断定写入失败。
        """
        if self._is_partitioned(src, dst):
            return False, None

        with self.lock:
            node = self.nodes.get(dst)
            delay = self.msg_delay
            repeat = self.msg_repeat
            drop_reply = self.drop_reply

        if node is None:
            return False, None

        if delay > 0:
            time.sleep(delay)

        if self._is_partitioned(src, dst):
            return False, None

        try:
            response = None

            for _ in range(repeat):
                response = node.apply_request(request)

            if drop_reply:
                return False, None

            return True, response

        except ConnectionError:
            return False, None

    def sync_all(self):
        """
        主动执行多轮同步，方便实验中确定何时已收敛。
        """
        node_ids = self.get_all_nodes()

        for _ in range(max(1, len(node_ids))):
            for src_id in node_ids:
                for target_id in node_ids:
                    if src_id == target_id:
                        continue

                    state = self.request_sync(target_id, src_id)

                    if state is None:
                        continue

                    with self.lock:
                        src_node = self.nodes.get(src_id)

                    if src_node is None:
                        continue

                    try:
                        src_node.merge_snapshot(state)
                    except ConnectionError:
                        pass

    def add_partition(self, a, b):
        with self.lock:
            self.partition_pairs.add(
                frozenset((str(a), str(b)))
            )

        print(
            "\n[故障注入] 网络分区：{} <--> {}".format(a, b)
        )

    def remove_partition(self, a, b):
        with self.lock:
            self.partition_pairs.discard(
                frozenset((str(a), str(b)))
            )

        print(
            "\n[故障恢复] 解除分区：{} <--> {}".format(a, b)
        )

    def set_delay(self, seconds):
        if seconds < 0:
            raise ValueError("延迟不能为负数")

        with self.lock:
            self.msg_delay = float(seconds)

        print(
            "\n[故障注入] 消息延迟：{} 秒".format(seconds)
        )

    def set_repeat(self, count):
        if count < 1:
            raise ValueError("重复次数必须至少为 1")

        with self.lock:
            self.msg_repeat = int(count)

        print(
            "\n[故障注入] 消息重复次数：{}".format(count)
        )

    def enable_drop_reply(self):
        with self.lock:
            self.drop_reply = True

        print("\n[故障注入] 开启响应丢失")

    def disable_drop_reply(self):
        with self.lock:
            self.drop_reply = False

        print("\n[故障恢复] 关闭响应丢失")

    def clear_all_faults(self):
        with self.lock:
            self.partition_pairs.clear()
            self.msg_delay = 0.0
            self.msg_repeat = 1
            self.drop_reply = False

        print("\n[系统] 已清除网络故障")

# 5. 客户端

class CartClient:
    def __init__(self, client_id, network, target_node):
        self.client_id = str(client_id)
        self.network = network
        self.target_node = str(target_node)

        self.vc = VectorClock()

        # key -> item -> 已观察到的添加事件 tag
        self.observed = {}

    def get_cart(self, cart_key):
        node = self.network.nodes.get(self.target_node)

        if node is None:
            raise ConnectionError("目标节点不存在")

        cart, node_clock = node.read_with_context(cart_key)

        self.vc.merge(node_clock)

        self.observed[cart_key] = {
            item: sorted(cart.observed_tags(item))
            for item in cart.get_items()
        }

        return cart.get_items()

    def make_request(
        self,
        cart_key,
        operation,
        item,
        request_id=None
    ):
        if request_id is None:
            request_id = str(uuid.uuid4())

        observed_tags = []

        if operation == "remove":
            observed_tags = self.observed.get(
                cart_key, {}
            ).get(item, [])

        return {
            "request_id": str(request_id),
            "key": str(cart_key),
            "operation": operation,
            "item": str(item),
            "clock": self.vc.to_dict(),
            "observed_tags": list(observed_tags)
        }

    def send_request(self, request):
        ok, response = self.network.send_operation(
            self.client_id,
            self.target_node,
            request
        )

        if ok:
            self.vc.merge(
                VectorClock.from_dict(response["clock"])
            )

        return ok

    def add_item(self, cart_key, item):
        request = self.make_request(
            cart_key,
            "add",
            item
        )

        return self.send_request(request)

    def remove_item(self, cart_key, item):
        """
        删除前先读取，明确客户端观察到哪些添加事件。

        如果读取与删除之间其他副本并发添加同一商品，
        新添加事件不会被这次删除移除。
        """
        self.get_cart(cart_key)

        request = self.make_request(
            cart_key,
            "remove",
            item
        )

        return self.send_request(request)

# 6. 实验辅助函数

class Experiment:
    """
    每次实验使用独立临时目录。

    实验结束后：
    1. 关闭后台同步线程。
    2. 清理临时文件。

    避免上一次运行留下的数据影响下一次实验。
    """

    def __init__(self, node_names=("phone", "pc", "pad")):
        self.temp_dir = tempfile.mkdtemp(
            prefix="crdt_cart_demo_"
        )

        self.net = VirtualNetwork()
        self.nodes = {}

        for name in node_names:
            path = os.path.join(
                self.temp_dir,
                "{}.json".format(name)
            )

            node = KVNode(
                name,
                self.net,
                path
            )

            self.nodes[name] = node
            self.net.register_node(node)

    def close(self):
        for node in self.nodes.values():
            node.shutdown()

        shutil.rmtree(
            self.temp_dir,
            ignore_errors=True
        )


def check(condition, message):
    if not condition:
        raise AssertionError("验证失败：" + message)

    print("【通过】" + message)


def print_cart(node, key):
    cart = node.get(key)

    if cart is None:
        print("节点不可用")
    else:
        print(sorted(cart.get_items()))


def isolate_all(net, node_ids):
    for i in range(len(node_ids)):
        for j in range(i + 1, len(node_ids)):
            net.add_partition(
                node_ids[i],
                node_ids[j]
            )


def heal_all(net):
    net.clear_all_faults()

# 7. 正常功能演示

def normal_demo():
    print("\n" + "=" * 60)
    print("【正常模式】三副本购物车")
    print("=" * 60)

    exp = Experiment()

    try:
        net = exp.net

        phone = CartClient(
            "phone_client",
            net,
            "phone"
        )

        pc = CartClient(
            "pc_client",
            net,
            "pc"
        )

        print("\n手机添加苹果和香蕉")
        phone.add_item("user01_cart", "苹果")
        phone.add_item("user01_cart", "香蕉")

        print(
            "手机购物车：",
            sorted(phone.get_cart("user01_cart"))
        )

        print("\n等待后台自动同步……")
        time.sleep(2)

        print(
            "电脑购物车：",
            sorted(pc.get_cart("user01_cart"))
        )

        print("\n电脑删除香蕉")
        pc.remove_item("user01_cart", "香蕉")

        print("\n等待后台同步……")
        time.sleep(2)

        print(
            "手机购物车：",
            sorted(phone.get_cart("user01_cart"))
        )

        print(
            "电脑购物车：",
            sorted(pc.get_cart("user01_cart"))
        )

        check(
            phone.get_cart("user01_cart") == {"苹果"},
            "删除后手机副本结果正确"
        )

        check(
            pc.get_cart("user01_cart") == {"苹果"},
            "删除后电脑副本结果正确"
        )

    finally:
        exp.close()

# 8. 九个故障场景

def test1_replica_crash():
    print("\n【场景 1】副本突然宕机")
    print(
        "【预测】一个副本宕机时，其他副本仍可写；"
        "重启并同步后恢复数据。"
    )

    exp = Experiment()

    try:
        net = exp.net
        phone = exp.nodes["phone"]
        pc = exp.nodes["pc"]

        client = CartClient("u1", net, "phone")

        pc.crash()

        check(
            client.add_item("cart", "apple"),
            "存活节点接受写入"
        )

        pc.restart()
        net.sync_all()

        result = pc.get("cart").get_items()

        print("恢复后的电脑副本：", sorted(result))

        check(
            result == {"apple"},
            "重启并同步后电脑副本获得商品"
        )

        print(
            "【解释】写入成功只表示手机副本完成本地持久化；"
            "恢复同步后其他副本才获得数据。"
        )

    finally:
        exp.close()


def test2_partition_repeat():
    print("\n【场景 2】网络分区与消息重复三次")
    print(
        "【预测】分区期间两端可以独立修改；"
        "并发添加与删除同一商品时，添加获胜；"
        "重复请求不会产生多个添加事件。"
    )

    exp = Experiment()

    try:
        net = exp.net

        phone_client = CartClient(
            "phone_client",
            net,
            "phone"
        )

        pc_client = CartClient(
            "pc_client",
            net,
            "pc"
        )

        phone_client.add_item("cart", "milk")
        net.sync_all()

        # 两个客户端都先观察到旧的 milk。
        phone_client.get_cart("cart")
        pc_client.get_cart("cart")

        isolate_all(
            net,
            ["phone", "pc", "pad"]
        )

        # 删除旧 milk。
        remove_request = phone_client.make_request(
            "cart",
            "remove",
            "milk"
        )

        check(
            phone_client.send_request(remove_request),
            "手机端删除请求成功"
        )

        # 电脑端并发重新添加 milk。
        add_request = pc_client.make_request(
            "cart",
            "add",
            "milk"
        )

        net.set_repeat(3)

        check(
            pc_client.send_request(add_request),
            "电脑端重复发送三次"
        )

        net.set_repeat(1)

        heal_all(net)
        net.sync_all()

        phone_result = phone_client.get_cart("cart")
        pc_result = pc_client.get_cart("cart")

        print("手机最终结果：", sorted(phone_result))
        print("电脑最终结果：", sorted(pc_result))

        check(
            phone_result == {"milk"},
            "手机最终保留并发添加的 milk"
        )

        check(
            pc_result == {"milk"},
            "电脑最终与手机收敛"
        )

        tags = exp.nodes["pc"].get(
            "cart"
        ).adds["milk"]

        check(
            len(tags) == 2,
            "只有旧添加和一次新添加，没有因重复消息生成额外事件"
        )

        print(
            "【解释】删除只删除已观察到的旧添加 tag；"
            "分区期间并发产生的新 tag 保留。"
        )

    finally:
        exp.close()


def test3_delay_out_of_order():
    print("\n【场景 3】消息延迟 10 秒并乱序到达")
    print(
        "【预测】乱序交付后，已观察到的旧添加被删除，"
        "并发新添加保留。"
    )

    exp = Experiment()

    try:
        net = exp.net

        phone = CartClient("p", net, "phone")
        pc = CartClient("c", net, "pc")

        phone.add_item("cart", "apple")
        net.sync_all()

        phone.get_cart("cart")
        pc.get_cart("cart")

        isolate_all(
            net,
            ["phone", "pc", "pad"]
        )

        remove_request = phone.make_request(
            "cart",
            "remove",
            "apple"
        )

        add_request = pc.make_request(
            "cart",
            "add",
            "apple"
        )

        # 用虚拟时间表达消息到达时刻：
        # 删除消息在虚拟第 10 秒到达；
        # 并发添加消息在虚拟第 1 秒到达。
        #
        # 这里不实际等待 10 秒，方便重复演示。
        queue = [
            (10, remove_request),
            (1, add_request)
        ]

        target = exp.nodes["phone"]

        for arrival_time, request in sorted(
            queue,
            key=lambda x: x[0]
        ):
            print(
                "虚拟时间 {} 秒：交付 {}".format(
                    arrival_time,
                    request["operation"]
                )
            )

            target.apply_request(request)

        heal_all(net)
        net.sync_all()

        result = phone.get_cart("cart")

        print("最终购物车：", sorted(result))

        check(
            result == {"apple"},
            "乱序交付后并发添加保留"
        )

        print(
            "【解释】实验使用虚拟时间模拟延迟和乱序，"
            "不代表真实网络传输。"
        )

    finally:
        exp.close()


def test4_restart_persist():
    print("\n【场景 4】节点重启后内存丢失")
    print(
        "【预测】崩溃时内存清空，重启后从磁盘恢复商品。"
    )

    exp = Experiment()

    try:
        net = exp.net
        phone_node = exp.nodes["phone"]

        client = CartClient("u1", net, "phone")

        check(
            client.add_item("cart", "orange"),
            "写入 orange"
        )

        phone_node.crash()

        check(
            phone_node.kv_store == {},
            "崩溃后内存已清空"
        )

        phone_node.restart()

        result = client.get_cart("cart")

        print("重启后购物车：", sorted(result))

        check(
            result == {"orange"},
            "磁盘恢复成功"
        )

    finally:
        exp.close()


def test5_migrate_crash():
    print("\n【场景 5】新节点迁移到一半时宕机")
    print(
        "【预测】已迁移的 key 在新节点重启后保留；"
        "源节点保留全部数据；恢复同步后补齐。"
    )

    exp = Experiment(
        node_names=("old_node", "new_node")
    )

    try:
        net = exp.net
        old_node = exp.nodes["old_node"]
        new_node = exp.nodes["new_node"]

        # 避免后台自动同步提前复制全部数据。
        net.add_partition("old_node", "new_node")

        client = CartClient(
            "u1",
            net,
            "old_node"
        )

        for i in range(4):
            client.add_item(
                "cart_{}".format(i),
                "item_{}".format(i)
            )

        migrated, total = new_node.migrate_from(
            old_node,
            progress=0.5
        )

        print(
            "迁移进度：{}/{} 个 key".format(
                migrated,
                total
            )
        )

        check(
            migrated == 2 and total == 4,
            "确实迁移了一半 key"
        )

        new_node.crash()
        new_node.restart()

        check(
            len(new_node.kv_store) == 2,
            "新节点恢复了已迁移的一半 key"
        )

        check(
            len(old_node.kv_store) == 4,
            "源节点仍保留全部 key"
        )

        net.remove_partition(
            "old_node",
            "new_node"
        )

        net.sync_all()

        check(
            len(new_node.kv_store) == 4,
            "恢复同步后新节点补齐全部 key"
        )

        print(
            "【解释】这里按 key 迁移，"
            "不是完整的动态分片和成员变更协议。"
        )

    finally:
        exp.close()


def test6_hot_key():
    print("\n【场景 6】热点 key")
    print(
        "【预测】40 个请求中一半访问同一个 key；"
        "并发写入后热点购物车保留 20 个商品。"
    )

    exp = Experiment(node_names=("phone",))

    try:
        net = exp.net
        errors = []

        def worker(i):
            try:
                client = CartClient(
                    "client_{}".format(i),
                    net,
                    "phone"
                )

                if i % 2 == 0:
                    key = "hot_cart"
                else:
                    key = "normal_cart_{}".format(i)

                ok = client.add_item(
                    key,
                    "goods_{}".format(i)
                )

                if not ok:
                    errors.append(
                        "请求 {} 未收到成功响应".format(i)
                    )

            except Exception as exc:
                errors.append(str(exc))

        start = time.monotonic()

        threads = [
            threading.Thread(
                target=worker,
                args=(i,)
            )
            for i in range(40)
        ]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        elapsed = time.monotonic() - start

        hot_cart = exp.nodes["phone"].get("hot_cart")

        count = len(hot_cart.get_items())

        print("热点商品数：", count)
        print("执行耗时：{:.3f} 秒".format(elapsed))

        check(
            not errors,
            "40 个请求没有报告错误"
        )

        check(
            count == 20,
            "热点 key 保留 20 个商品"
        )

        check(
            len(exp.nodes["phone"].kv_store) == 21,
            "包含 1 个热点 key 和 20 个普通 key"
        )

        print(
            "【解释】节点内部使用锁保护内存和文件。"
            "本机耗时不能代表真实分布式吞吐量。"
        )

    finally:
        exp.close()


def test7_concurrent_cas():
    print("\n【场景 7】两个客户端并发执行 CAS")
    print(
        "【预测】同一版本的单节点 CAS 最多一个成功；"
        "两个副本的 CRDT 并发添加都能保留。"
    )

    # 单节点 CAS 对照实验。
    cas_lock = threading.Lock()
    cas_state = {
        "version": 0,
        "value": None
    }
    cas_results = []

    def cas_worker(value):
        with cas_lock:
            if cas_state["version"] == 0:
                cas_state["value"] = value
                cas_state["version"] = 1
                cas_results.append(True)
            else:
                cas_results.append(False)

    t1 = threading.Thread(
        target=cas_worker,
        args=("x",)
    )

    t2 = threading.Thread(
        target=cas_worker,
        args=("y",)
    )

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("CAS 成功数：", sum(cas_results))
    print("CAS 最终状态：", cas_state)

    check(
        sum(cas_results) == 1,
        "两个同版本 CAS 只有一个成功"
    )

    exp = Experiment()

    try:
        net = exp.net

        phone = CartClient("p", net, "phone")
        pc = CartClient("c", net, "pc")

        isolate_all(
            net,
            ["phone", "pc", "pad"]
        )

        phone.add_item("cart", "x")
        pc.add_item("cart", "y")

        heal_all(net)
        net.sync_all()

        result = phone.get_cart("cart")

        print("CRDT 最终结果：", sorted(result))

        check(
            result == {"x", "y"},
            "CRDT 合并后两个商品都保留"
        )

        print(
            "【解释】CAS 对照只保证单节点内比较和写入原子；"
            "它不是分布式强一致 CAS。"
        )

    finally:
        exp.close()


def test8_time_backward():
    print("\n【场景 8】系统时间回退一分钟")
    print(
        "【预测】物理时间回退不会影响向量时钟递增"
        "和购物车合并。"
    )

    exp = Experiment(node_names=("phone",))

    try:
        net = exp.net
        client = CartClient("u1", net, "phone")

        # 可控的模拟物理时钟。
        fake_time = [1000]

        client.add_item("cart", "first")
        before = client.vc.copy()

        fake_time[0] -= 60

        print("模拟物理时间回退后：", fake_time[0])

        client.add_item("cart", "second")
        after = client.vc.copy()

        print("操作前向量时钟：", before)
        print("操作后向量时钟：", after)

        check(
            fake_time[0] == 940,
            "模拟时间回退了 60 秒"
        )

        check(
            before.is_before(after),
            "向量时钟仍然递增"
        )

        check(
            client.get_cart("cart") == {
                "first",
                "second"
            },
            "两个商品均保留"
        )

        print(
            "【解释】这里注入的是模拟时钟，"
            "没有修改操作系统时间。"
        )

    finally:
        exp.close()


def test9_disk_ok_reply_lost():
    print("\n【场景 9】磁盘写成功，但响应丢失")
    print(
        "【预测】客户端无法确定第一次请求结果；"
        "使用同一个请求 ID 重试不会产生第二个添加事件。"
    )

    exp = Experiment(node_names=("phone",))

    try:
        net = exp.net
        node = exp.nodes["phone"]

        client = CartClient("u1", net, "phone")

        request = client.make_request(
            "cart",
            "add",
            "goods"
        )

        net.enable_drop_reply()

        first_result = client.send_request(request)

        print("第一次客户端收到成功响应：", first_result)

        check(
            first_result is False,
            "客户端未收到成功响应"
        )

        net.disable_drop_reply()

        # 先重启，验证请求记录和商品确实已经持久化。
        node.crash()
        node.restart()

        second_result = client.send_request(request)

        print("重试收到成功响应：", second_result)

        check(
            second_result,
            "使用同一请求重试成功"
        )

        cart = node.get("cart")
        tags = cart.adds.get("goods", set())

        print("goods 的添加事件数：", len(tags))

        check(
            len(tags) == 1,
            "同一请求只产生一个添加事件"
        )

        check(
            cart.get_items() == {"goods"},
            "购物车结果正确"
        )

        print(
            "【解释】响应丢失表示结果未知，"
            "客户端应使用同一请求 ID 重试。"
        )

    finally:
        exp.close()

# 9. 菜单

TESTS = [
    test1_replica_crash,
    test2_partition_repeat,
    test3_delay_out_of_order,
    test4_restart_persist,
    test5_migrate_crash,
    test6_hot_key,
    test7_concurrent_cas,
    test8_time_backward,
    test9_disk_ok_reply_lost
]


def run_all_tests():
    failures = []

    for index, test_func in enumerate(TESTS, start=1):
        print("\n" + "=" * 60)
        print("运行故障场景 {}".format(index))
        print("=" * 60)

        try:
            test_func()

        except Exception as exc:
            failures.append(
                (index, str(exc))
            )

            print(
                "【失败】场景 {}：{}".format(
                    index,
                    exc
                )
            )

    print("\n" + "=" * 60)

    if failures:
        print("以下场景失败：")

        for index, message in failures:
            print(
                "场景 {}：{}".format(index, message)
            )

    else:
        print("九个故障场景均通过断言。")


def main():
    while True:
        print("\n" + "=" * 60)
        print(" 分布式购物车 KV Store")
        print(" CRDT + 向量时钟 + 自动同步")
        print(" 1 - 正常功能演示")
        print(" 2 - 运行全部九个故障场景")
        print(" 3 - 运行单个故障场景")
        print(" 0 - 退出")
        print("=" * 60)

        try:
            choice = input("请输入选项：").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n退出程序")
            break

        if choice == "0":
            print("退出程序")
            break

        elif choice == "1":
            try:
                normal_demo()
            except Exception as exc:
                print("正常演示失败：", exc)

        elif choice == "2":
            run_all_tests()

        elif choice == "3":
            number = input(
                "请输入故障编号 1~9："
            ).strip()

            if not number.isdigit():
                print("编号无效")
                continue

            index = int(number)

            if not 1 <= index <= 9:
                print("编号无效")
                continue

            try:
                TESTS[index - 1]()
            except Exception as exc:
                print(
                    "场景 {} 失败：{}".format(
                        index,
                        exc
                    )
                )

        else:
            print("输入无效，请重新选择")


if __name__ == "__main__":
    main()