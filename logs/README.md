# logs 对github.com/sadlil/gologger的二次封装

## 说明
+ 无需声明logger对象
+ 使用方法类似slf4j的logger，格式中用{}替换传入值的内容
+ 可以使用config初始化配置中，在配置文件中定义 go.log.level为 debug,info,warn,error

## 安装
go get -u github.com/kgin/logs

## 使用范例
```go
    str := "测试"
    m := map[string]any{"userid": 1,"username":"mmaacc"}
    logs.Debug("测试日志,str:{},m:{}",str,m)
```