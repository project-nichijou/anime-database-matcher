# 介绍

本项目为[Project Nichijou](https://github.com/project-nichijou)中的子项目，进行不同数据源数据库之间的匹配工作。

# 原理

参考：
- [文档/Matcher实现细节](https://github.com/project-nichijou/intro/blob/master/doc.md#matcher-%E5%AE%9E%E7%8E%B0%E7%BB%86%E8%8A%82)

# 配置方法

- `database/database_settings_template.py`复制、重命名为`database_settings.py`，并配置相应字段。
- `plan.txt`中记录需要匹配的数据库名称，用换行分隔

# 使用方法

```
$ python3 main.py 
Usage: main.py [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  match  match `target`
  run    perform matching task
```

```
$ python3 main.py run --help
Usage: main.py run [OPTIONS]

  perform matching task

Options:
  -f, --config PATH  match configuration file. `plan.txt` will be used if not specified.
  --help             Show this message and exit.
```
