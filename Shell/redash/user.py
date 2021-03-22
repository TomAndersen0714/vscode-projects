import json

# zjk-growth-01 growth user/dept

ins = {
    '提效工程组',
    '客户运营组',
    '客服提能组',
    'CDE对接部',
    '会话改进组',
    '渠道支撑组',
    '配置中台组',
    '智能跟单组',
    '架构组',
    '算法组',
    'BI组',
    '客户端组',
    '智能会话部'
}

user_items = []
with open("./user.json", mode="r", encoding="UTF-8") as fi:
    for i in fi.readlines():
        user_items.append(json.loads(i.strip("\n")))

dept_items = []
with open("./dept.json", mode="r", encoding="UTF-8") as fi:
    for i in fi.readlines():
        dept_items.append(json.loads(i.strip("\n")))
depts = {}
for i in dept_items:
    depts[i["dept_id"]] = i
users = []
for i in user_items:
    name = i["name"]
    email = i["email"]
    departments = i["departments"]
    if email == '' or len(departments) > 1 or len(departments) == 0:
        continue
    if departments[0] not in depts:
        continue
    dept_info = depts[departments[0]]
    dept_name = dept_info["name"]
    if dept_name.startswith("客户端"):
        dept_name = '客户端组'
    if dept_name not in ins:
        continue
    if 'test' in name:
        continue
    user = email.split("@")[0]
    users.append({
        'user_id': i["user_id"],
        'name': name,
        'user': user,
        'email': email,
        'dept': dept_name
    })
print(len(users))
for i in users:
    print(i)
with open('user_dept.json', mode='w', encoding='UTF-8') as fi:
    json.dump(users, fi, ensure_ascii=False)
