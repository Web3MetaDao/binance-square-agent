"""
research — 量化机构策略自动挖掘 → 深度学习优化 → 实盘验证系统。

子系统：
  - harvester.py       数据采集调度器
  - store.py           SQLite 策略库（schema + CRUD）
  - sources/           各数据源采集器（github / arxiv / blog / kaggle）
  - parsers/           文档 → 结构化解析（Hermes调用）
  - fusion/            策略融合优化 + 过拟合审核
  - sources/__init__.py 采集器注册
  - parsers/__init__.py 解析器注册
"""
