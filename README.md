分布式系统概念与设计-课程作业

### 功能概述

多台机器利用真实数据集中的数据来实现pagerank算法，计算数据来源于

https://snap.stanford.edu/data/wiki-Vote.html，目的是获得维基百科编辑管理人员，采用用户投票机制，采用pagerank的算法思想对人员进行排名。

**基本步骤要求如下：**

1. 利用链接里给的真实图数据 Dataset
2. 把数据load到至少两台机器上
3. 实现两个机器上的并行pagerank，每个机器负责算自己内存中的节点，然后两个机器同步数据，再算下一轮节点的rank值
4. 每次节点同步完成，再算下一轮值前，把内存中节点rank值存储到两台机器的磁盘上（checkpoint），然后挂掉一台机器，再重启起来，把重启机器中的内存数据从checkpoint中读出来，然后继续计算

