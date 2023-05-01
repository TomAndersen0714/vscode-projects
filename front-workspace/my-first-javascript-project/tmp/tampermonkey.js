// ==UserScript==
// @name         百度搜索测试
// @namespace    http://tampermonkey.net/
// @version      1.0
// @description  代码功能：百度搜索123，返回456的搜索结果
// @author       You
// @match        https://www.baidu.com/*
// @grant        none
// ==/UserScript==


// a one-time called function, i.e. self-invoking function
(function () {
    console.log('加载脚本');

    /**
     * 拦截并修改ajax请求
     */
    window.beforeXMLHttpRequestOpen = function (xhr, options) {
        console.log('before open', xhr);
        // 修改url
        options.url = options.url.replace('wd=123', 'wd=456');
        // 修改method
        // options.method = 'POST';
    };
    /**
     * 拦截并修改ajax请求
     */
    window.beforeXMLHttpRequestSend = function (xhr, body) {
        console.log('before send', xhr);
        // 修改请求头
        xhr.setRequestHeader('key1', 'value1');
    };

    /**
     * 重写open方法
     * https://developer.mozilla.org/zh-CN/docs/Web/API/XMLHttpRequest/open
     */
    XMLHttpRequest.prototype.myOpen = XMLHttpRequest.prototype.open;
    XMLHttpRequest.prototype.open = function (method, url, async, user, password) {
        // 用对象便于修改参数
        var options = {
            method: method, url: url, async: async, user: user, password: password
        };
        if ('function' === typeof window.beforeXMLHttpRequestOpen) {
            window.beforeXMLHttpRequestOpen(this, options);
        }
        this.myOpen(options.method, options.url, options.async);
    };

    /**
     * 重写send方法
     * https://developer.mozilla.org/zh-CN/docs/Web/API/XMLHttpRequest/send
     */
    XMLHttpRequest.prototype.mySend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.send = function (body) {
        if ('function' === typeof window.beforeXMLHttpRequestSend) {
            window.beforeXMLHttpRequestSend(this, body);
        }
        this.mySend(body);
    };

})();
