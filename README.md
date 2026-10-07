# 无线电频谱干扰调查与协调

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8332`。

模块结构：`app.py` 负责组装，`src/domain.py` 定义字段和错误，`src/rules.py` 负责评估、定位、授权和状态机，`src/repository.py` 管理 SQLite、版本和审计链，`src/service.py` 编排权限，`src/http_api.py` 提供接口，`src/audit.py` 生成审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8332
python3 -m unittest discover -s tests -v
```

使用 `X-User-Id`、`X-Role`、`X-Region` 请求头。接口为 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions`、`GET /api/items/<id>/audit`、`GET /api/merge-groups`、`GET /api/merge-groups/<id>`、`POST /api/merge-groups/<id>/merge`、`GET|POST /api/items/<id>/planned-actions` 和 `POST /api/sync`。

归并账：同一频段（容差 2MHz）、同一地点（监测站相同）、十五分钟内的重复上报会自动进入待归并区（`merge_groups`）。协调人（coordinator）选定主事件后归并：测量记录（sources）与未执行的拟办动作整批转到主事件，已执行动作只留在原事件记录里、不再重放；原事件保留来源编号并标为 `merged`。归并采用乐观锁（`expected_version`），两名协调人同时归并同一组时只留先写入的一份；跨辖区归并返回 `region_mismatch`（403）。主事件状态或管辖区域变更后，待归并选点与转办动作失效并重算。断网时先记本地，回网按 `group_no` 调 `POST /api/sync` 合并，按 `action_key` 幂等去重，重复不新增动作。测试覆盖完整调查流程、测量更正、重复事件、跨区越权、定位置信度、版本冲突、归并转账、并发归并、失效重算与断网同步。
