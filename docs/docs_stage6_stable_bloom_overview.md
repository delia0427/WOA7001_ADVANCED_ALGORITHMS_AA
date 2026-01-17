# 阶段6：稳定布隆过滤器（Stable Bloom Filter, SBF）

目的
- 为“低置信 + 高读数”等可疑模式提供短期记忆，减少重复告警或作为风险加权的辅助信号
- 旧的可疑设备会随时间被衰减，数据结构长期保持稳定容量

工作机制（本实现）
- 计数数组（m 个 b 位计数器）
- 插入(item):
  - 随机选择 `decay_per_insert` 个位置，若计数>0 则减一（全局老化）
  - 对 item 的 k 个哈希位置，将计数置为最大值（“新鲜”）
- 查询(item):
  - 若 k 个位置计数都 > 0，则“近期可能出现过”（允许误报）；否则一定未出现

与 HLL/CMS 的关系
- HLL：去重的“高置信异常设备数”（阈值：高读数且置信达标）
- SBF：宽松的“可疑设备短期记忆”（阈值：低置信 + 高读数）
- CMS：频次直方图（region × bucket）
- 三者可联合用于证据融合：广泛异常（CMS↑）/设备扩散（HLL↑）/可疑多发（SBF↑）

参数与建议
- num_cells（m）：越大误报率越低、记忆容量越大
- num_hashes（k）：一般 3–6；过大增加碰撞依赖
- counter_bits（b）：计数上限 2^b−1；4–8 位常用
- decay_per_insert：每次插入衰减的随机位置数；越大遗忘越快、稳态更稀疏
- 哈希使用 SHA-256 双重哈希，跨运行稳定

典型规则（可在配置 thresholds 中设定）
- `sbf_low_conf_max`: 0.50
- `sbf_high_reading_min`: 与 `anomaly_reading` 一致或略低

监控指标
- `saturation`: 非零计数比例；过高可能导致误报率上升
- 结合窗口内 SBF 写入次数、HLL 去重计数、CMS 重频桶数量一同观察

# 稳定布隆过滤器（SBF）配置示例

```yaml
stable_bloom:
  num_cells: 8192        # 计数数组长度 m
  num_hashes: 4          # 哈希次数 k
  counter_bits: 4        # 每格计数位宽 b（最大计数 2^b-1）
  decay_per_insert: 2    # 每次插入时随机衰减的位置数
  seed: 1337             # 随机种子

thresholds:
  anomaly_reading: 145.0
  min_confidence_for_hll: 0.60
  sbf_low_conf_max: 0.50
  sbf_high_reading_min: 145.0
```

说明
- 若 SBF 的 `saturation` 持续偏高，可增大 `num_cells` 或 `decay_per_insert`，或收紧写入规则。
- 若希望记忆更久，降低 `decay_per_insert` 或增大 `counter_bits`（更高的“新鲜度”上限）。