# WOA7001_ADVANCED_ALGORITHMS_AA

# Case A — 方法实现的步骤计划
目标：按题目四问与评分标准，分阶段完成“随机化流式异常检测管线 + 概率重建 + 基线对比 + 约束评估”。每步给出要实现的方法集合、依赖关系与产出。

## 与题目四问的对应关系
- 第1问（随机化管线）：阶段1–8、12
- 第2问（概率重建）：阶段9
- 第3问（与确定性方法对比）：阶段10–11、12
- 第4问（批判性评估）：阶段12–13

---

## 阶段0：项目骨架与类型
- 任务
  - 创建项目结构与配置文件：configs/config.yaml
  - 定义数据类型：SensorRecord、ImputedRecord、WindowStats（src/pipeline/types.py）
- 产出
  - 可导入的类型模块；配置可读取

---

## 阶段1：管线装配与通路打通（最小可跑）[DONE]
- 任务
  - Pipeline.__init__：装配 Imputer、ReservoirSampler、CountMinSketch、HyperLogLog、LSHIndex、StableBloomFilter、MonteCarloEstimator、AlertEvaluator
  - Pipeline.process_stream：串联调用并返回空 alerts
  - load_config/main/fake_stream：加载配置，构造演示数据流
- 依赖
  - 阶段0完成
- 产出
  - main() 可执行，打印空告警

---

## 阶段2：插补（基础版）[DONE]
- 任务（src/modules/imputation/imputer.py）
  - Imputer.impute：在 reading 缺失/损坏时生成 reading_hat（使用简单回填策略作为占位）
  - Imputer.fetch_neighborhood：返回同区域邻居（占位）
- 产出
  - 记录进入管线时都有 reading_hat

---

## 阶段3：加权蓄水池采样 [DONE]
- 任务（src/modules/sampling/reservoir.py）
  - ReservoirSampler.insert：A-ES 加权关键值逻辑（不写具体代码）
  - ReservoirSampler.sample：返回按关键值排序的样本
- 依赖
  - Pipeline._score：score = reading_hat × confidence × risk_multiplier
- 产出
  - 每窗口有代表性样本集

---

## 阶段4：素描（CMS/HLL） [DONE]
- 任务（src/modules/sketches/count_min.py、hll.py）
  - CountMinSketch.__init__/update/query：近似计数与参数（ε/δ）
  - HyperLogLog.add/estimate：去重估计不同异常传感器数
  - Pipeline._bucket：读数分桶
- 依赖
  - 阶段1–3完成
- 产出
  - 可查询近似频次与基数估计

---

## 阶段5：LSH 近似聚类  [DONE]
- 任务（src/modules/lsh/lsh.py）
  - LSHIndex.__init__/insert/query：生成签名、入桶与候选查询
  - LSHIndex.bucket_stats：输出各桶统计（大小、均值、方差、异常占比）
- 依赖
  - 记录有 features（reading_hat、confidence、risk 等）
- 产出
  - 近似簇与桶级统计可用

---

## 阶段6：Stable Bloom 伪造过滤  [SBF]
- 任务（src/modules/filters/stable_bloom.py）
  - StableBloomFilter.__init__/add/contains/decay：标记低置信高读数可疑 ID，维持稳定大小
- 依赖
  - Pipeline 阈值：low_confidence、anomaly_reading
- 产出
  - 可疑设备集合及时间衰减逻辑

---

## 阶段7：窗口聚合与 Monte Carlo 估计
- 任务（src/modules/estimation/monte_carlo.py、Pipeline._estimate_window）
  - MonteCarloEstimator.estimate：对区域/桶样本估计异常率 p̂ 与置信区间
  - MonteCarloEstimator.aggregate_regions：汇总区域级指标
  - Pipeline._estimate_window：收集 reservoir/LSH/CMS 的样本，生成 WindowStats 列表
- 依赖
  - 阶段3–5完成；有样本与桶统计
- 产出
  - 区域/簇级统计与区间

---

## 阶段8：告警评估
- 任务（src/modules/alerts/evaluator.py）
  - AlertEvaluator.should_alert：区间下界 > 区域阈值、最小样本量、一致性
  - AlertEvaluator.combine_evidence：CMS/HLL/LSH/Reservoir 多源证据融合
- 依赖
  - 阶段7的 WindowStats
- 产出
  - 警报列表与规则解释

---

## 阶段9：概率重建（题目第2问，S204/S209 展示）
- 任务（src/modules/imputation/imputer.py 扩展）
  - Imputer.estimate_region_weighted_mean：邻域加权均值（权重依 confidence/相似度）
  - Imputer.robust_trimmed_mean：修剪加权均值（抗极端/伪造）
  - Imputer.mixture_posterior_estimate：常态+异常混合后验（低置信场景）
  - Imputer.estimate_uncertainty_interval：点估计区间（Wilson/Bootstrap 或正态近似）
- 依赖
  - fetch_neighborhood 能返回邻域集合
- 产出
  - 对 S204（R3）与 S209（R2）给出 reading_hat、区间与理由（用于论文示例）

---

## 阶段10：确���性基线实现（对比两种方法）
- 任务（新增 baseline 模块，方法名即可）
  - BaselineThresholdEMA.run_window：固定阈值 + EMA 滑动平均 + 全量计数
  - BaselineOfflineClustering.run_batch：离线 K-means/DBSCAN 近似（或 Space-Saving/Misra–Gries 重频算法）
- 依赖
  - 与同样数据流/窗口对齐
- 产出
  - 基线结果（异常率/误报漏报/延迟/内存）

---

## 阶段11：实验场景与指标采集
- 任务（新增 ExperimentRunner）
  - ExperimentRunner.run_scenarios：噪声/对抗场景（丢包、低置信、伪造 spike、集中噪声）
  - 指标：Precision、Recall、FPR、CI 下界、吞吐（记录/秒）、延迟（ms）、内存（MB）
  - MetricsReporter.report_cms_hll：素描摘要上报（用于观测与调参）
- 依赖
  - 管线与基线均可运行
- 产出
  - 场景化结果表与图（供论文对比）

---

## 阶段12：参数调优与误差/复杂度陈述
- 任务
  - 调整 ε/δ、Reservoir K、LSH L/k、SBF 容量/衰减、阈值等
  - 给出误差上界（CMS/HLL）、复杂度（单记录 O(1)~O(log K)）、内存预算
- 产出
  - 参数选择理由与表格，误差/资源权衡文字说明

---

## 阶段13：批判性评估与文档整理
- 任务
  - 真实约束下评估：准确性 vs 效率、扩展到 1.2M、极端噪声爆发、方法局限与改进
  - 整理文档（框架、架构图、参考文献），将 S204/S209 重建与小算例纳入正文
- 产出
  - 作业主体稿件（≤10 页）与参考文献清单

---

## 阶段14：最终联调与提交
- 任务
  - 端到端运行演示流；生成 alerts 与统计摘要
  - 对比基线；导出图表与结果
- 产出
  - 可复现的实验与结论；提交材料

---


建议分层实现

必要的骨架（先实现）
管线与配置：流程 glue 代码（process_stream、_bucket、_score、阈值读取）
插补基础版：有值直传；缺失用区域均值/全局均值回退
CMS 计数：实现或用库（关键路径，用来得到直方图）
简单告警：基于阈值和比例的规则（先不做复杂融合）
占位/替代（先跑通，再迭代）
HLL：先用 set 去重占位，后续换成标准 HLL
Reservoir：可暂时跳过或用简易采样（非关键即可不启用）
LSH：可推迟（仅当需要“相似簇”视角时再上）
SBF：可推迟（需要“低置信+高读数”的短期记忆时再上）
后续增强（迭代时补）
高级插补（邻域加权/稳健/后验）
Monte Carlo/估计与区间
证据融合与告警抑制（CMS/HLL/LSH/SBF 合并）
逐阶段是否“必须写代码”的简表

阶段1 插补：需要，但可很简单（直传+均值回退）
阶段2 打分/分桶/管线：需要（小函数+流程 glue）
阶段3 蓄水池：可选（不影响主功能）
阶段4 CMS：建议实现/引入库（核心统计）
阶段5 HLL：可先用 set，占位即可
阶段6 LSH：可选（有相似簇诉求再做）
阶段7 SBF：可选（需短期记忆再做）
阶段8 Monte Carlo：可后置（先用简单比例/规则）
阶段9 告警融合：需要基础规则；复杂融合后置
最小可用版本的组成

插补基础版 + 分桶 + CMS 查询 + 简单阈值告警
HLL 用 set 占位（估计“异常设备数”）
后续再按需求引入 Reservoir、LSH、SBF、Monte Carlo 与融合
这样做可以快速跑通主流程，资源可控、风险低，再逐步把复杂阶段替换为正式实现。

---

阶段0：项目骨架与类型

任务
创建项目结构与配置文件：configs/config.yaml
定义数据类型：SensorRecord、ImputedRecord、WindowStats（src/pipeline/types.py）
产出
可导入的类型模块；配置可读取
阶段1：管线装配与通路打通（最小可跑）

任务
Pipeline.init：装配 Imputer、ReservoirSampler、CountMinSketch、HyperLogLog、LSHIndex、StableBloomFilter、MonteCarloEstimator、AlertEvaluator
Pipeline.process_stream：串联调用并返回空 alerts
load_config/main/fake_stream：加载配置，构造演示数据流
依赖
阶段0完成
产出
main() 可执行，打印空告警
阶段2：插补（基础版）

任务（src/modules/imputation/imputer.py）
Imputer.impute：在 reading 缺失/损坏时生成 reading_hat（使用简单回填策略作为占位）
Imputer.fetch_neighborhood：返回同区域邻居（占位）
产出
记录进入管线时都有 reading_hat
阶段3：加权蓄水池采样

任务（src/modules/sampling/reservoir.py）
ReservoirSampler.insert：A-ES 加权关键值逻辑（不写具体代码）
ReservoirSampler.sample：返回按关键值排���的样本
依赖
Pipeline._score：score = reading_hat × confidence × risk_multiplier
产出
每窗口有代表性样本集
阶段4：素描（CMS/HLL）

任务（src/modules/sketches/count_min.py、hll.py）
CountMinSketch.init/update/query：近似计数与参数（ε/δ）
HyperLogLog.add/estimate：去重估计不同异常传感器数
Pipeline._bucket：读数分桶
依赖
阶段1–3完成
产出
可查询近似频次与基数估计
阶段5：LSH 近似聚类

任务（src/modules/lsh/lsh.py）
LSHIndex.init/insert/query：生成签名、入桶与候选查询
LSHIndex.bucket_stats：���出各桶统计（大小、均值、方差、异常占比）
依赖
记录有 features（reading_hat、confidence、risk 等）
产出
近似簇与桶级统计可用
阶段6：Stable Bloom 伪造过滤

任务（src/modules/filters/stable_bloom.py）
StableBloomFilter.init/add/contains/decay：标记低置信高读数可疑 ID，维持稳定大小
依赖
Pipeline 阈值：low_confidence、anomaly_reading
产出
可疑设备集合及时间衰减逻辑
阶段7：窗口聚合与 Monte Carlo 估计

任务（src/modules/estimation/monte_carlo.py、Pipeline._estimate_window）
MonteCarloEstimator.estimate：对区域/桶样本估计异常率 p̂ 与置信区间
MonteCarloEstimator.aggregate_regions：汇总区域级指标
Pipeline._estimate_window：收集 reservoir/LSH/CMS 的样本，生成 WindowStats 列表
依赖
阶段3–5完成；有样本与桶统计
产出
区域/簇级统计与区间
阶段8：告警评估

任务（src/modules/alerts/evaluator.py）
AlertEvaluator.should_alert：区间下界 > 区域阈值、最小样本量、一致性
AlertEvaluator.combine_evidence：CMS/HLL/LSH/Reservoir 多源证据融合
依赖
阶段7的 WindowStats
产出
警报列表与规则解释
阶段9：已忽略（保留编号空缺）

说明
根据你的指示，本版本不包含“概率重建”阶段。若未来需要，可将其并入阶段2（高级插补）或新增扩展章节，不影响其他阶段依赖。
阶段10：确定性基线实现（对比两种方法）

任务（新增 baseline 模块，方法名即可）
BaselineThresholdEMA.run_window：固定阈值 + EMA 滑动平均 + 全量计数
BaselineOfflineClustering.run_batch：离线 K-means/DBSCAN 近似���或 Space-Saving/Misra–Gries 重频算法）
依赖
与同样数据流/窗口对齐
产出
基线结果（异常率/误报漏报/延迟/内存）
阶段11：实验场景与指标采集

任务（新增 ExperimentRunner）
ExperimentRunner.run_scenarios：噪声/对抗场景（丢包、低置信、伪造 spike、集中噪声）
指标：Precision、Recall、FPR、CI 下界、吞吐（记录/秒）、延迟（ms）、内存（MB）
MetricsReporter.report_cms_hll：素描摘要上报（用于观测与调参）
依赖
管线与基线均可运行
产出
场景化结果表与图（供论文对比）
阶段12：参数调优与误差/复杂度陈述

任务
调整 ε/δ、Reservoir K、LSH L/k、SBF 容量/衰减、阈值等
给出误差上界（CMS/HLL）、复杂度（单记录 O(1)~O(log K)）、内存预算
产出
参数选择理由与表格，误差/资源权衡文字说明
阶段13：批判性评估与文档整理

任务
真实约束下评估：准确性 vs 效率、扩展到 1.2M、极端噪声爆发、方法局限与改进
整理文档（框架、架构图、参考文献），将小算例纳入正文
产出
作业主体稿件（≤10 页）与参考文献清单
阶段14：最终联调与提交

任务
端到端运行演示流；生成 alerts 与统计摘要
对比基线；导出图表与结果
产出
可复现的实验与结论；提交材料
与题目四问的对应关系（去除概率重建后）

第1问（随机化管线）：阶段1–8、12
第2问（概率重建）：本版本忽略；如需覆盖，可在后续将其并入阶段2的“高级插补”扩展
第3问（与确定性方法对比）：阶段10–11、12
第4问（批判性评估）：阶段12–13
如需我把“概率重建”并回到阶段2（作为“高级插补扩展”）并更新依赖图，我也可以提供一个“含概率重建但不占用独立阶段9”的版本。

You said: 之前你已经给的阶段1-10 继续采用，我让你在阶段10之后的发的阶段9：概率重建（题目第2问，S204/S209 展示） 这个先忽略，你可以考虑加在后面的阶段。按这样的逻辑重新构建阶段
之前你已经给的阶段1-10 继续采用，我让你在阶段10之后的发的阶段9：概率重建（题目第2问，S204/S209 展示） 这个先忽略，你可以考虑加在后面的阶段。按这样的逻辑重新构建阶段
Copilot said: 好的，继续采用你之前已经确认的阶段1–10不变，把“概率重建”暂时忽略并顺延到后续阶段。下面是按这个逻辑重构的完整阶段计划（不含具体代码），每阶段给出任务、依赖与产出。 阶段1：管线装配与通路打通（最小可跑） - 任务 - Pipeline.__init__：装配 Imputer、ReservoirSampler、CountMinSketch、HyperLogLog、LSHIndex、StableBloomFilter、MonteCarloEstimator、AlertEvaluator、Reporting Sinks
好的，继续采用你之前已经确认的阶段1–10不变，把“概率重建”暂时忽略并顺延到后续阶段。下面是按这个逻辑重构的完整阶段计划（不含具体代码），每阶段给出任务、依赖与产出。

阶段1：管线装配与通路打通（最小可跑）

任务
Pipeline.init：装配 Imputer、ReservoirSampler、CountMinSketch、HyperLogLog、LSHIndex、StableBloomFilter、MonteCarloEstimator、AlertEvaluator、Reporting Sinks
Pipeline.process_stream：串联调用核心组件（先不汇总告警）
load_config/main/fake_stream：加载配置，构造演示数据流
依赖
项目骨架与类型（已具备）
产出
main() 可执行，流程跑通（当前窗口返回空告警）
阶段2：插补（基础版）

任务
Imputer.impute：在 reading 缺失/损坏时生成 reading_hat（简单回填/区域均值占位）
Imputer.fetch_neighborhood：返回同区域邻居（占位实现）
产出
流入管线的记录都具备 reading_hat（低置信场景先走占位策略）
阶段3：加权蓄水池采样

任务
ReservoirSampler.insert：A‑ES 加权关键值逻辑（方法签名与流程定义）
ReservoirSampler.sample：返回按关键值排序的样本
依赖
Pipeline._score：score = reading_hat × confidence × risk_multiplier
产出
每窗口得到代表性样本集
阶段4：素描（CMS/HLL）

任务
CountMinSketch.init/update/query：近似计数；参数 ε/δ 与保守更新
HyperLogLog.add/estimate：去重估计“异常设备数”（高读数且置信达标）
Pipeline._bucket：读数分桶（region × bucket 复合键）
依赖
阶段1–3完成
产出
近似频次与基数估计可查询
阶段5：LSH 近似聚类

任务
LSHIndex.init/insert/query：随机超平面签名、入桶与候选查询
LSHIndex.bucket_stats：输出各签名桶统计（大小、均值等）
依赖
记录具备 features（reading_hat、confidence、risk_multiplier 等）
产出
近似簇与桶级统计
阶段6：Stable Bloom 伪造过滤

任务
StableBloomFilter.init/add/contains/decay：标记“低置信+高读数”可疑 ID，维持稳定容量
依赖
Pipeline 阈值：low_confidence、anomaly_reading
产出
可疑设备近似集合及衰减逻辑（饱和度可观察）
阶段7：窗口聚合与 Wilson 估计

任务
MonteCarloEstimator.aggregate_regions_from_cms：基于 CMS 的区域异常率 p̂ 与 Wilson 区间
标记重频桶（f(b)/total ≥ heavy_ratio_threshold）
依赖
阶段3–5完成；有样本与桶统计
产出
每区域 WindowStats（p̂、CI、重频桶）
阶段8：Bootstrap + 多源证据融合

任务
MonteCarloBootstrapEstimator.aggregate_with_evidence：
基��� Reservoir 做加权 bootstrap（样本充足时）
融合 CMS（重频强度/占比）、HLL（异常设备数）、SBF（饱和度），生成 evidence_score
样本不足时回退 Wilson 区间
依赖
阶段7的统计 + Reservoir 样本
产出
综合窗口统计（Stage8WindowStats：p̂、CI、cms_total/cms_abnormal、heavy_buckets、hll_estimate、sbf_saturation、evidence_score）
阶段9：告警评估与证据融合

任务
AlertEvaluator.generate_alerts：
数据量门槛（cms_total、sample_size）
三类触发规则（CI下界、p̂+重频、分数+设备扩散）
分数定级（severity）与冷却（cooldown）
依赖
阶段8的 Stage8WindowStats
产出
Alert 列表（severity、score、reason、metrics、thresholds）

阶段10：结果落地与可观察性

任务
Reporting Sinks（JSONL/CSV/Stdout/Webhook）：write_window_stats / write_alerts
Pipeline.export_stage10：运行阶段8/9并统一写出
依赖
阶段8–9完成
产出
外部可消费的统计与告警数据（含审计字段）


阶段11：概率重建（高级插补扩展，原“阶段9”后移）

任务
Imputer 扩展方法（方法名与接口约定，无需立即给代码）：
estimate_region_weighted_mean（邻域加权均值）
robust_trimmed_mean（修剪加权均值）
mixture_posterior_estimate（常态+异常混合后验）
estimate_uncertainty_interval（Bootstrap/正态近似区间）
可选示例：S204（R3）与 S209（R2）重建输出（reading_hat、区间、理由）
依赖
fetch_neighborhood 能返回邻域集合
产出
低置信/缺失场景的概率重建结果与解释；与管线集成的高级插补路径


阶段12：确定性基线实现（对比两种方法）

任务
BaselineThresholdEMA.run_window：固定阈值 + EMA 滑动平均 + 全量计数
BaselineOfflineClustering.run_batch：离线 K‑means/DBSCAN 近似（或 Space‑Saving/Misra–Gries 重频）
依赖
与同样数据��/窗口对齐
产出
基线结果与资源占用（异常率/误报漏报/延迟/内存）
阶段13：实验场景与指标采集

任务
ExperimentRunner.run_scenarios：噪声/对抗场景（丢包、低置信、伪造 spike、集中噪声）
指标：Precision、Recall、FPR、CI 下界覆盖率、吞吐（记录/秒）、延迟（ms）、内存（MB）
MetricsReporter.report_cms_hll：素描摘要上报（用于观测与调参）
依赖
管线与基线均可运行
产出
场景化结果表与图（供论文对比）


阶段14：参数调优与误差/复杂度陈述

任务
调整 ε/δ、Reservoir K、LSH L/k、SBF 容量/衰减、阈值等
给出误差上界（CMS/HLL）、复杂度（单记录 O(1)~O(log K)）、内存预算
产出
参数选择理由与表格，误差/资源权衡说明
阶段15：批判性评估与文档整理

任务
真实约束评估：准确性 vs 效率、扩展到 1.2M、极端噪声爆发、方法局限与改进
整理文档（架构、流程图、参考文献），纳入必要小算例与案例说明
产出
作业主体稿件（≤10 页）与参考文献清单
阶段16：最终联调与提交

任务
端到端运行演示流；生成 alerts 与统计摘要
对比基线；导出图表与结果
产出
可复现的实验与结论；提交材料
与题目四问的对应关系（采用1–10原编号，概率重建后移到阶段11）

第1问（随机化管线）：阶段1–10、14
第2问（概率重建）：阶段11
第3问（与确定性方法对比）：阶段12–13、14
第4问（批判性评估）：阶段14–15