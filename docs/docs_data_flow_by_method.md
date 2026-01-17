# 项目数据流与各方法的转换说明（不含具体代码）

目标：逐个方法说明数据如何在管线中被转换、生成哪些字段、产生哪些副作用或下游输入。并以具体样例记录给出逐步流转轨迹。

---

## 总览：一条记录的典型流转路径

SensorRecord → Imputer.impute → ImputedRecord
→ Pipeline._risk_multiplier → 填充 `risk_multiplier`
→ Pipeline._score → 计算 `score`
→ ReservoirSampler.insert(score) → 进入代表性样本池
→ Pipeline._bucket(reading_hat) → 生成 `(region, bucket)` 键
→ CountMinSketch.update(key) → 更新区域×区间的近似频次
→ 条件满足时 HyperLogLog.add(sensor_id) → 去重的异常设备计数
→ 可选（后续阶段）：LSHIndex.insert(features, sensor_id) → 构建相似桶
→ 可选（后续阶段）：StableBloomFilter.add(suspect_id) → 标记可疑设备
→ 窗口结束：Pipeline._estimate_window + MonteCarloEstimator → 得到 WindowStats
→ AlertEvaluator.should_alert(stats) → 生成告警决策

---

## 方法级数据流与转换

### 1) src/modules/imputation/imputer.py

- Imputer.impute(record: SensorRecord) → ImputedRecord
  - 输入：`sensor_id, region, reading?, confidence, metadata`
  - 转换：
    - 若 `reading` 存在 → `reading_hat = reading`
    - 若 `reading` 缺失/损坏 → 用 `_fallback_mean(region)` 生成基础 `reading_hat`（后续阶段可替换为邻域加权/稳健/混合后验）
  - 输出：`ImputedRecord(sensor_id, region, reading_hat, confidence, risk_multiplier=占位, features=[])`
  - 下游：Pipeline 为其补充 `risk_multiplier` 与特征 `features`

- Imputer.fetch_neighborhood(record) → List[SensorRecord]
  - 输入：当前记录
  - 转换：占位（后续阶段返回同区域/近邻的历史/当前片段）
  - 下游：供高级插补方法使用（加权均值、修剪均值、混合后验）

- Imputer.estimate_region_weighted_mean(region, neighbors) → float
  - 输入：区域与邻域集合
  - 转换：按 `confidence/距离/相似度` 加权求均值，输出点估计（S204 示例用）
  - 下游：作为 `reading_hat` 的候选

- Imputer.robust_trimmed_mean(values, weights, trim_ratio) → float
  - 输入：邻域值与权重
  - 转换：剔除上下极端分位后加权求均值，提升抗噪/抗伪造
  - 下游：作为 `reading_hat` 的稳健候选

- Imputer.mixture_posterior_estimate(region, neighbors, priors) → float
  - 输入：区域邻域与先验（常态+异常成分）
  - 转换：计算后验期望或众数，适于低置信高风险场景（S209 示例用）
  - 下游：作为 `reading_hat` 的概率候选

- Imputer.estimate_uncertainty_interval(mean_hat, var_hat, confidence_level) → (low, high)
  - 输入：点估计与方差估计
  - 转换：输出不确定性区间（正态近似/自助法）
  - 下游：在 WindowStats 或重建章节中展示

---

### 2) src/pipeline/pipeline.py（与类型）

- Pipeline._risk_multiplier(region: str) → float
  - 输入：区域名
  - 转换：查配置 `risk_multipliers[region]`，无则用 `default`
  - 下游：写入 `ImputedRecord.risk_multiplier`

- Pipeline._score(rec: ImputedRecord) → float
  - 输入：插补后的记录
  - 转换：`score = reading_hat × confidence × risk_multiplier`
  - 下游：传给 `ReservoirSampler.insert(item, weight)`

- Pipeline._bucket(reading_hat: float, width=5.0) → int
  - 输入：连续读数
  - 转换：离散分桶 `bucket_index = floor(reading_hat / width)`
  - 下游：与区域拼接为 `key = (region, bucket)` 用于 CMS

- Pipeline.process_stream(stream, window_id) → List[WindowStats]
  - 输入：一段流（Iterable[SensorRecord]）
  - 转换：
    - 对每条记录：impute → 填风险倍数 → 计算特征与 score
    - reservoir.insert(记录, score)
    - cms.update((region, bucket), 1)
    - 条件满足（读数高且置信度达标）→ hll.add(sensor_id)
    - 可选后续：lsh.insert(features, sensor_id)、sbf.add(suspect)、…
  - 输出：当前阶段返回空统计（后续在窗口结束汇总）
  - 下游：窗口结束调用 `_estimate_window` 生成 WindowStats

- Pipeline._estimate_window(window_id) → List[WindowStats]
  - 输入：窗口上下文（样本、桶统计、HLL估计等）
  - 转换：汇总区域/簇的异常率估计与区间（借助 MonteCarloEstimator）
  - 输出：窗口统计，供告警评估

---

### 3) src/modules/sampling/reservoir.py

- ReservoirSampler.insert(item: ImputedRecord, weight: float)
  - 输入：代表性打分与记录
  - 转换：生成 A‑ES 关键值 `key = u^(1/weight)`；维护大小 K 的堆，仅保留 key 最大的 K 个样本
  - 输出：无（内部状态改变）
  - 下游：`sample()` 返回代表性样本列表用于窗口估计/可视化/分析

- ReservoirSampler.sample() → List[ImputedRecord]
  - 输入：无
  - 转换：按代表性从高到低排序
  - 输出：当前窗口的样本子集
  - 下游：MonteCarlo/统计汇总

---

### 4) src/modules/sketches/count_min.py

- CountMinSketch.update(key=(region, bucket), amount=1)
  - 输入：区域×离散桶键与增量
  - 转换：对 depth 行的对应列执行保守/标准更新；近似频次增加
  - 输出：无（内部计数表更新）
  - 下游：`query(key)` 返回该键的近似频次；`estimate_heavy_buckets(threshold)` 识别重频桶

- CountMinSketch.query(key) → int
  - 输入：键
  - 转换：取各行该列的最小值作为近似频次
  - 输出：频次上界估计
  - 下游：用于区域直方图与异常强度证据

---

### 5) src/modules/sketches/hll.py

- HyperLogLog.add(sensor_id: str)
  - 输入：设备 ID（条件：读数高且置信度达标）
  - 转换：写入去重结构（占位用集合；后续替换为寄存器）
  - 输出：无（内部状态改变）
  - 下游：`estimate()` 返回不同异常设备数

- HyperLogLog.estimate() → int
  - 输入：无
  - 转换：估计基数（占位为集合大小）
  - 输出：窗口内不同异常传感器数量
  - 下游：与区域异常率、CMS 直方图联合评估

---

### 6) src/modules/lsh/lsh.py（后续阶段）

- LSHIndex.insert(vec: features, item_id: str)
  - 输入：特征向量（如 `[reading_hat, confidence, risk, delta]`）与设备ID
  - 转换：生成 LSH 签名，插入多个表，形成近似相似桶
  - 输出：无（内部桶结构变化）
  - 下游：`bucket_stats()` 用于簇级统计；`query(vec)` 查近邻候选

- LSHIndex.query(vec, top_k) → List[item_id]
  - 输入：向量与 k
  - 转换：在签名匹配的桶中检索候选并（可选）再筛
  - 输出：相似记录 ID 列表
  - 下游：局部异常率估计/聚类分析

- LSHIndex.bucket_stats() → List[stats]
  - 输入：无
  - 转换：输出各桶的大小、均值、方差、异常比例
  - 输出：簇级证据
  - 下游：MonteCarlo/AlertEvaluator

---

### 7) src/modules/filters/stable_bloom.py（后续阶段）

- StableBloomFilter.add(item_id)
  - 输入：可疑设备 ID（规则：低置信 + 高读数等）
  - 转换：置位并参与时间衰减
  - 输出：无（内部状态改变）
  - 下游：`contains(item_id)` 作为告警或权重调整的辅助信号

- StableBloomFilter.contains(item_id) → bool
  - 输入：设备 ID
  - 转换：查询是否处于可疑集合（允许误报）
  - 输出：布尔
  - 下游：规则引擎/权重调整

- StableBloomFilter.decay()
  - 输入：无
  - 转换：周期性衰减位数组，移除陈旧标记
  - 输出：无
  - 下游：维持过滤器稳定大小与新鲜度

---

### 8) src/modules/estimation/monte_carlo.py（后续阶段）

- MonteCarloEstimator.estimate(bucket_records) → (p_hat, ci)
  - 输入：一个区域或簇内的样本/记录集合
  - 转换：随机子样本/自助法估计异常率 `p̂` 与置信区间
  - 输出：点估计与区间
  - 下游：窗口统计与告警阈值比较

- MonteCarloEstimator.aggregate_regions(region_buckets) → List[WindowStats]
  - 输入：多个区域/桶的样本集合
  - 转换：逐区域汇总 `p̂` 与区间
  - 输出：WindowStats 列表
  - 下游：AlertEvaluator

---

### 9) src/modules/alerts/evaluator.py（后续阶段）

- AlertEvaluator.should_alert(window_stats: WindowStats) → bool
  - 输入：区域/簇统计（包含 p̂ 与 CI）
  - 转换：规则判断（CI 下界 > 阈值、最小样本量、一致性）
  - 输出：布尔（是否告警）
  - 下游：生成告警事件/记录

- AlertEvaluator.combine_evidence(stats_list) → score/decision
  - 输入：CMS/HLL/LSH/Reservoir 的多源证据
  - 转换：融合打分与一致性检验
  - 输出：总体决策或置信度
  - 下游：最终告警与报告

---

## 三个具体样例的流转轨迹

- 样例 A：S204（R3，reading 缺失，confidence=0.88）
  1) SensorRecord → Imputer.impute：`reading_hat = _fallback_mean("R3") = 151.0`
  2) _risk_multiplier("R3") → 1.3（来自配置）
  3) _score → `151.0 × 0.88 × 1.3 ≈ 172.8`
  4) Reservoir.insert(item=S204_imputed, weight=172.8)
  5) _bucket(151.0, 5) → 30；key=("R3", 30)；CMS.update(key,+1)
  6) HLL.add? 读数 151 > anomaly_reading=145 且置信度 0.88 ≥ 0.60 → add("S204")
  7) 后续窗口汇总：S204 参与 R3 的 p̂ 估计与 CI

- 样例 B：S203（R2，reading=149，confidence=0.18，metadata: spoof）
  1) Impute：`reading_hat = 149`
  2) _risk_multiplier("R2") → 1.2
  3) _score → `149 × 0.18 × 1.2 ≈ 32.2`
  4) Reservoir.insert(weight=32.2)（代表性较低，可能不保留）
  5) _bucket(149,5)=29；key=("R2",29)；CMS.update(key,+1)
  6) HLL.add? 尽管读数高，但 `confidence=0.18 < min_confidence_for_hll=0.60` → 不加入 HLL
  7) 后续阶段（可选）：满足“低置信+高读数”规则 → SBF.add("S203") 标记可疑

- 样例 C：S211（R2，reading=96，confidence=0.80）
  1) Impute：`reading_hat = 96`
  2) _risk_multiplier("R2") → 1.2
  3) _score → `96 × 0.80 × 1.2 ≈ 92.2`
  4) Reservoir.insert(weight=92.2)
  5) _bucket(96,5)=19；key=("R2",19)；CMS.update(key,+1)
  6) HLL.add? 96 ≤ 145 → 不加入
  7) 作为常态样本参与窗口直方图与 p̂ 估计的对照

---

## 字段与事件一览（进入/退出）

- 进入管线（SensorRecord）：
  - 必有：`sensor_id, region, confidence`
  - 可缺：`reading`
- 生成的主字段（ImputedRecord）：
  - 新增：`reading_hat, risk_multiplier, features=[reading_hat, confidence, delta?, risk]`
- 结构更新事件：
  - Reservoir：插入代表性样本（按 score）
  - CMS：更新 `(region, bucket)` 的近似频次
  - HLL：条件加入 `sensor_id`，用于异常设备去重
  - LSH/SBF：后续阶段用于相似簇与可疑过滤
- 窗口结束：
  - MonteCarlo → `p̂` 与 CI
  - AlertEvaluator → 告警与解释

---

## 与“连续值离散分区”的关系

- `_bucket(reading_hat, width)` 将连续读数映射为固定宽度的区间索引；与 `region` 组合形成键，专用于 CMS 的近似直方图。
- 宽度越小，分辨率越高但桶数增加；宽度越大，更稳健但分辨率下降。可依据噪声水平调参。
