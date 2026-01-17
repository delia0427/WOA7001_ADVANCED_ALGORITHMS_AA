# 1. A randomised detection pipeline: Sketching
## 1.1 Principle explanation and processing flow 
Count-Min Sketch (CMS) 是一种用于估计数据流中事件频率的数据结构[引用]。其核心思想是构造一个二维计数器数组，通过多个哈希函数将输入键映射到数组各行的特定列位置。更新时，每行对应的列位置增加计数值；查询时，取所有行对应位置的最小值作为频率估计。
Alon-Matias-Szegedy (AMS)【引用】是在内存极为有限的情况下，从海量数据流（Data Stream）中实时估算整体的汇总统计量，特别是计算开销高昂的频率二阶矩（F₂）。
HyperLogLog (HLL) is a method that uses extremely small memory to quickly estimate how many non-repetitive elements there are in massive data. [Reference]
威尔逊（Wilson）置信区间是一种专门用于估计比例的稳健方法。
## 1.2
结合了以上多种草图技术及多维度监控手段，以“并行计算、多维聚合”的核心理念设计架构图【如图】。【表一】说明了各个模块的功能。
【删除】这个架构将CMS、AMS、HLL三条流独立处理，对所有数据进行频次（CMS）和爆发度（AMS）统计，HLL仅处理高置信度异常，聚合时得到的cms_total（总量）、p̂（异常比例）、hll_estimate（波及范围）、ams_f2（分布重尾）共同构成了对系统状态的立体画像。
## 1.3 复杂度说明
下表为单条更新和窗口聚合的复杂度说明。
【表格】
单条更新均由常数级操作组成，适合高吞吐。窗口聚合的复杂度与桶数和区域数成线性关系，但可通过固定桶数、并行计算实现高效处理。
## 1.4 聚合数据说明

## 1.5 结果
为了验证Sketching算法准确性，采用离线验证的计算逻辑和其对比。离线计算逻辑是精确、完整的全量计算，两个计算方法将读取一样的数据源和配置文件计算，并对比结果。计算逻辑和对比标准如表所示

