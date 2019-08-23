#!/usr/bin/python3
'''
to be used to generate a pile of images for import to deployment-prep commons
requires: pycairo
'''
import sys
import cairo


def write_png(font, canvas_width, glyph, output_path):
    '''
    given an output file path, a font face, the width of the
    output image (height will be same as the width) and the glyph
    to be displayed, create an image with a 2 pixel border, a yellowish-tan
    background and the centered glyph in black.
    '''
    canvas_height = canvas_width

    # for bitmap output. all units in pixels
    surface = cairo.ImageSurface(cairo.FORMAT_RGB24, canvas_width, canvas_height)
    ctx = cairo.Context(surface)

    # background
    ctx.rectangle(0, 0, canvas_width - 1, canvas_height - 1)
    ctx.set_source_rgb(0.9, 0.8, 0.6)
    ctx.fill()
    # border
    ctx.set_line_width(2)
    ctx.set_source_rgb(0.1, 0.1, 0.1)
    ctx.stroke()

    # glyph
    ctx.set_source_rgb(0, 0, 0)
    ctx.set_font_size(canvas_width - 2)
    ctx.select_font_face(font, cairo.FONT_SLANT_NORMAL, cairo.FONT_WEIGHT_BOLD)
    (x_bearing, y_bearing, text_width, text_height, _dx, _dy) = ctx.text_extents(glyph)
    # print("x_advance is", _dx, "and y_advance is", _dy)
    # print("text_width is", text_width, "and text_height is", text_height)
    # print("x_bearing is", x_bearing, "and y_bearing is", y_bearing)

    options = cairo.FontOptions()
    options.set_antialias(cairo.ANTIALIAS_DEFAULT)
    ctx.set_font_options(options)

    ctx.move_to(canvas_width/2 - text_width/2 - x_bearing,
                canvas_height/2 - text_height/2 - y_bearing)
    ctx.show_text(glyph)

    surface.write_to_png(output_path)


def usage(message=None):
    '''display usage info about this script'''
    if message is not None:
        print(message)
    print("Usage: generate_glyph_pngs.py font-name canvas-width-in-px glyph output-file-path")
    print("Canvas width and height are the same, font is always bold weight, colors are fixed")
    print("Example use: python3 generate_glyph_pngs.py 'Noto Serif CJK JP' 32 見  me.png")
    sys.exit(1)


def do_main():
    '''
    entry point
    '''
    if len(sys.argv) != 5:
        usage('missing or extra arg(s)')
    font = sys.argv[1]
    canvas_width = sys.argv[2]
    if not canvas_width.isdigit():
        usage('width must be the number of pixels')
    glyph = sys.argv[3]
    output_path = sys.argv[4]

    write_png(font, int(canvas_width), glyph, output_path)


if __name__ == '__main__':
    do_main()
