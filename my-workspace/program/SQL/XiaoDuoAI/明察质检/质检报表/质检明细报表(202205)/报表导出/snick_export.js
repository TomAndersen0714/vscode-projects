var od = getData('61dd6a0e15c11556fbeda736', true)
if (!od || od.length === 0) {
    return
}

let label_fixed_titles = ["平台", "店铺", "子账号分组", "客服姓名", "上级姓名", "客服子账号"],
    stat_fixed_titles = ["总会话量", "平均分", "AI质检量", "AI异常会话量", "AI扣分分值", "AI扣分会话比例", "AI加分会话量", "AI加分分值", "AI加分会话比例", "建议抽检量", "人工抽检量", "抽检比例", "人工扣分会话量", "人工扣分分值", "人工扣分会话比例", "人工加分会话量", "人工加分分值", "人工加分会话比例", "自定义扣分会话量", "自定义扣分分值", "自定义扣分会话比例", "自定义加分会话量", "自定义加分分值", "自定义加分会话比例"],
    ai_fixed_titles = ["非客服结束会话", "漏跟进", "快捷短语重复", "生硬拒绝", "欠缺安抚", "答非所问", "单字回复", "单句响应慢", "产品不熟悉", "活动不熟悉", "内部回复慢", "回复严重超时", "撤回人工消息", "单表情回复", "异常撤回", "转接前未有效回复", "超时未回复", "顾客撤回", "前后回复矛盾", "撤回机器人消息", "第三方投诉或曝光", "顾客提及投诉或举报", "差评或要挟差评", "反问/质疑顾客", "违禁词", "客服冷漠讥讽", "顾客怀疑假货", "客服态度消极敷衍", "售后不满意", "需求挖掘", "商品细节解答", "卖点传达", "商品推荐", "退换货理由修改", "主动跟进", "无货挽回", "活动传达", "店铺保障", "催拍催付", "核对地址", "好评引导", "优秀结束语", "满意", "感激", "期待", "对客服态度不满", "对发货物流不满", "对产品不满", "其他不满意", "顾客骂人", "对收货少件不满", "客服骂人"]

let keys = {}, values = [], titles = []
titles = titles.concat(label_fixed_titles).concat(stat_fixed_titles).concat(ai_fixed_titles)

keys = {}
od.forEach(d => {
    if (d['人工质检标签']) {
        d['人工质检标签'].split('$$').forEach(k => {
            keys[k] = 1
        })
    }

})
titles = titles.concat(Object.keys(keys))

keys = {}
od.forEach(d => {
    if (d['自定义质检标签']) {
        d['自定义质检标签'].split('$$').forEach(k => {
            keys[k] = 1
        })
    }
})
titles = titles.concat(Object.keys(keys))

od.forEach((d) => {

    var k = d['AI质检标签'] ? d['AI质检标签'].split('$$') : []
    var v = d['AI质检触发次数'] ? d['AI质检触发次数'].split('$$') : []
    var cr = {}
    k.forEach((i, ii) => {
        cr[i] = v[ii]
    })

    k = d['人工质检标签'] ? d['人工质检标签'].split('$$') : []
    v = d['人工质检触发次数'] ? d['人工质检触发次数'].split('$$') : []
    k.forEach((i, ii) => {
        cr[i] = v[ii]
    })

    k = d['自定义质检标签'] ? d['自定义质检标签'].split('$$') : []
    v = d['自定义质检触发次数'] ? d['自定义质检触发次数'].split('$$') : []
    k.forEach((i, ii) => {
        cr[i] = v[ii]
    })
    values.push(cr)
})

var excelData = [titles]
od.forEach((d, i) => {
    excelData.push(titles.map(t => {
        let v = od[i][t] || values[i][t];
        if (!v) {
            if (label_fixed_titles.indexOf(t) >= 0) return String()
            if (titles.indexOf(t) >= 0) return String(0)
        }
        return String(v)
    }))
})
excel(excelData, '质检报表-客服')