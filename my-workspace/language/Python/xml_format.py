import xml.etree.ElementTree as ET

def sort_xml_elements(element):
    # 对当前元素的子元素按标签名称进行排序
    element[:] = sorted(element, key=lambda child: child.tag)
    
    # 递归地对每个子元素进行排序
    for child in element:
        sort_xml_elements(child)

def sort_xml_file(input_file, output_file):
    # 解析XML文件
    tree = ET.parse(input_file)
    root = tree.getroot()
    
    # 对根元素进行排序
    sort_xml_elements(root)
    
    # 保存排序后的XML文件
    tree.write(output_file, encoding='utf-8', xml_declaration=True)