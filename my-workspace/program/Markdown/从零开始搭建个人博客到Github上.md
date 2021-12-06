## 前言

- 本次搭建的博客为基于Hexo框架的静态博客
- 本次是在Windows上搭建个人博客，其他系统大同小异，使用的是git bash来运行一些简单的Linux命令
- Github国内访问速度较慢（特殊工具除外），所以这并不是最佳的搭建方案，有条件的话建议还是自己购买服务器进行搭建

----



## 具体搭建步骤

### 1. 下载Git

- **本次搭建博客，我们主要会用到git bash工具来执行Linux命令，使用cmd也行。前往Git官网下载Git工具，并在git bash中配置好git（设置全局用户名、全局邮箱等），如：**

  ```bash
  git config --global user.name "John Doe"
  git config --global user.email johndoe@example.com
  ```

- **没有Github账号记得先申请一个，后续所有命令都在git bash中执行**

### 2. 下载node.js

- **前往node.js官网下载LTS版本（长期支持版），直接安装即可，因为后续Hexo的安装要用到npm工具：**

<img src="C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211221539675.png" alt="下载node.js" style="zoom:80%;" />

- **下载完成之后直接安装，安装完成之后使用 `node -v` 和 `npm -v` 命令来检查是否安装成功：**

![检查node.js安装](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211221915258.png)

### 3. 下载npm淘宝镜像

- **由于自带的npm工具下载hexo很慢（GFW牛逼~），所以先使用npm来下载cnpm工具，然后用cnpm下载hexo就会快得多：**

  ```bash
  npm install -g cnpm --registry=https://registry.npm.taobao.org
  ```

- **如果命令执行失败了就多试几次。安装完成后使用`cnpm -v`来测试是否安装成功：**

![安装cnpm](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211222529855.png)

### 4. 下载hexo框架

- **使用cnpm工具下载hexo：**

  ```bash
  cnpm install -g hexo-cli
  ```

- **使用 `hexo v` 命令来测试hexo是否安装成功：**

![安装hexo](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211222807438.png)

### 5. 启动hexo

- **先创建指定文件夹，用于hexo博客站点，本次我创建的文件夹名为`HexoBlogs`，之后的git bash命令都在此文件夹内执行：**

![创建博客站点文件夹](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211222918923.png)

- **进入此文件夹，启动hexo框架之前先进行初始化：**

  ```bash
  hexo init
  ```

- **初始化完成之后会创建一个默认的hello-world博客和默认的landscape主题。然后我们生成博客对应的页面**

  ```bash
  hexo g # 或者hexo generate
  ```

- **然后我们启动hexo服务（退出时命令行使用ctrl+c）**

  ```bash
  hexo s # 或者hexo server
  ```

- **前往`http://localhost:4000`页面查看本地博客页面是否生成成功：**

![localhost:4000](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211224029908.png)

### 6. 在Github上新建仓库

- **新建仓库命名格式必须为`<Owner>.github.io`，即“用户名.github.io”的格式，如`tomandersen-cc.github.io`：**

![新建仓库](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211224435794.png)

- **创建完成之后保留此页面：**

![保留页面](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211224509989.png)

### 7. 上传本地hexo博客框架

- **安装配置工具：**

  ```bash
  cnpm install --save hexo-deployer-git 
  ```

- **修改hexo相关配置，在之前创建的文件夹中，修改配置文件`_config.yml`，在最后几行的`deploy`模块中设置对应的参数`type` 、`repo`和`branch`：**

![配置参数](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211224712556.png)

- **其中参数`repo`的值即为之前创建仓库页面所显示的仓库地址，即：**

![repo参数](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211224812988.png)

- **修改完成后，保存退出**

- **然后重新生成博客页面并上传至Github，依次输入以下命令：**

  ```bash
  hexo clean
  hexo g # 或者 hexo generate
  hexo d # 或者 hexo deploy
  ```

- **通过之前设置的仓库名来访问上传的博客（之后仓库名不能更改）：**

![访问博客](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211225038937.png)



- **至此hexo博客就已经搭建完成，并且成功上传到Github了**

----



## 发布博客

- **使用`hexo n <blogname>`命令创建博客，如：`hexo n "My first blog"`：**

![发布博客](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211225352508.png)

- **会在hexo文件夹下的source/_posts/路径下创建对应名的.md文档，使用Markdown语法编辑此文档，然后再次创建博客页面，上传到Github即可实现博客发布：**

  ```bash
  hexo clean
  hexo g # 或者 hexo generate
  hexo d # 或者 hexo deploy
  ```

-----



## 更换博客主题

### 1. 下载主题

- **去Github检索相关主题，当然也可以去官方主题市场进行下载，这里选择Github上的[ material-indigo ](https://github.com/yscoder/hexo-theme-indigo)主题作为例。**

- **使用git bash执行`git clone`命令，将工程克隆到之前创建的博客文件夹下的`themes/indigo`路径下：**

  ```bash
  git clone https://github.com/yscoder/hexo-theme-indigo.git themes/indigo
  ```

- **若clone速度太慢可以尝试修改hosts文件或者其他方式，这里就不多赘述**

### 2. 配置主题

- **修改博客站点文件夹下的`_config.yml`文件，将其中的`theme`参数设置成新下载的主题名：**

![修改theme变量](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211230752803.png)

- **然后依旧是重新生成博客页面`hexo clean` `hexo g`，开启`hexo s`开启服务，在本地查看是否配置成功：**

![本地检查配置](C:\Users\DELL\AppData\Roaming\Typora\typora-user-images\image-20200211231339386.png)

- **最后便可以使用`hexo d`命令上传至Github，实现博客发布**

- **具体博客主题相关配置参考对应主题的官方文档**



# End~

