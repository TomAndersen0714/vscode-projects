if(!data||data.length===0){
    return []
}
let ks = {}
data.forEach(d=>{
    if(d['AI质检标签']){
        d['AI质检标签'].split('$$').forEach(k=>{
            ks[k]=1
        })
    }
})
ks = Object.keys(ks)
return data.map(d=>{
    var k = d['AI质检标签']?d['AI质检标签'].split('$$'):[]
    var v = d['AI质检触发次数']?d['AI质检触发次数'].split('$$'):[]
    var cr  = {},r={}
    k.forEach((i,ii)=>{
        cr[i]=v[ii]
    })
    var c = ['店铺','子账号分组','客服姓名','上级姓名']
    c.forEach(i=>{
        r[i]=d[i]
    })
    ks.forEach(i=>{
        r[i] = cr[i]||0
    })
    return r
})