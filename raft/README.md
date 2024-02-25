# 简介

Raft PartA 调试总结

## 第一关：实验测试

执行命令

```
VERBOSE=0 go test -run PartA | tee out.txt
```

## 第二关：并发测试

安装辅助测试插件

dslogs 和 dstest

脚本内容见 tools，在 mac 上：

```
sudo cp dslogs dstest /usr/local/bin
brew install python
pip3 install typer rich
```

执行 `dslogs -c 3 out.txt` 高亮分区查看日志
执行 `dstest PartA -p 30 -n 100` 进行并行测试

输入下面结果表示测试成功：

![](2024-02-05-02-24-49.png)