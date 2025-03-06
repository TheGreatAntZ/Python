from PIL import Image
import fire


def convert_m2n(m, n, x):
    """将 0-k 范围的数字转换为 0-n
    """
    return int(n / m * x)


def get_char_linear(r, g, b, k=None, ascii_char=None):
    """将像素点转换为符号
    """
    length = len(ascii_char)
    ascii_index = convert_m2n(256 * 3, length, r + g + b)
    return ascii_char[ascii_index]


def image_process(
        input_file='/Users/yogo/Desktop/需求/工作交接/交接文档（吴智敏）/数据分析/图像处理/Snipaste_2022-11-15_17-46-21.jpg',
        output_file='res.txt',
        pic_height=100,
        pic_width=100):
    # 步骤一, 读取图片并调整图片大小
    im = Image.open(input_file)
    im = im.resize((pic_width, pic_height), Image.NEAREST)
    # 步骤二, 定义使用的符号集
    ascii_char = list("你好呀，我是小王。　")
    # 步骤三, 遍历像素点, 将每一个像素点转换为符号并保存到文件
    txt = ""
    for i in range(pic_height):
        for j in range(pic_width):
            txt += get_char_linear(*im.getpixel((j, i)), ascii_char=ascii_char)
            txt += '  '
        txt += '\n'
    # 字符画输出到文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(txt)


if __name__ == '__main__':
    """
    input_file: 图像的名称
    output_file: 保存文件的名称
    """
    fire.Fire(image_process)
