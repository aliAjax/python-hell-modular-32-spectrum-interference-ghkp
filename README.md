# 无线电频谱干扰调查与协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8332`。

模块结构：`app.py` 负责组装，`src/domain.py` 定义字段和错误，`src/rules.py` 负责评估、定位、授权和状态机，`src/repository.py` 管理 SQLite、版本和审计链，`src/service.py` 编排权限，`src/http_api.py` 提供接口，`src/audit.py` 生成审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8332
python3 -m unittest discover -s tests -v
```

使用 `X-User-Id`、`X-Role`、`X-Region` 请求头。核心接口：

- `GET /health`
- `GET /api/state`
- `POST /api/items`
- `POST /api/items/<id>/sources`
- `POST /api/items/<id>/actions`
- `GET /api/items/<id>/audit`
- `POST /api/items/<id>/pending-actions`：先登记未执行动作
- `POST /api/pending-actions/<id>/execute`：携带主事件 `expected_version` 后执行
- `GET /api/merge-groups`、`GET /api/merge-groups/<group_no>/candidates`
- `POST /api/merge-groups/<group_no>/master`：协调人携带 `master_item_id`、`expected_version` 定主事件
- `POST /api/merge-groups/rebuild`：重算待归并选点
- `POST /api/offline-sync`：断网台账按离线 `group_no` 回网合并，按 `client_ref`、`client_action_id` 幂等

同区域、同频段、相近地点且检测时间相差 15 分钟内的事件进入待归并区。定主事件后，测量记录复制到主事件并保留来源事件编号；已执行动作只进入 `merge_action_refs` 账本，不重放；未执行动作改挂主事件。主事件候选状态或管辖区域变化会使旧选点失效。两个协调人并发定主事件时，归并组状态和版本保证只接受先写入的一笔。测试覆盖完整调查流程、测量更正、重复事件、跨区越权、定位置信度、版本冲突、归并账、离线重放和待归并重算。协议接入、真实无线电传播模型和执法权限仍需由外部系统实现。
