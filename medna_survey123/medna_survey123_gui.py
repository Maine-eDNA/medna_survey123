"""
medna_survey123_gui
Create tkinter gui for running medna_survey123_clean
Created By: mkimble
LAST MODIFIED: 09/15/2021
"""

import tkinter as tk
import tkinter.ttk as ttk
from tkinter import filedialog
from PIL import Image, ImageTk
from tkinter import N, E, S, W, messagebox

light_grey = '#616161'
dark_purple = '#8700e6'
light_purple = '#bf77ff'
blue = '#0044ff'


def init_styles():
    print('init styles')
    s = ttk.Style()
    s.theme_use('clam')
    print(s.theme_names(), s.theme_use())

    s.configure('TFrame', background=light_grey)
    s.configure('TLabel', font=('Quicksand', 12, 'roman'), background=light_grey)
    s.configure('TButton', padding=2, font=('Quicksand', 10, 'roman'))
    s.configure('TEntry')
    s.configure('TLabelframe', background=light_grey)
    s.configure('TCheckbutton', background=light_grey, font=('Quicksand', 10, 'roman'))

    s.configure('large.TLabel', background=light_grey, font=('Quicksand', 14, 'roman'))
    s.configure('medium.TLabel', background=light_grey, font=('Quicksand', 12, 'roman'))
    s.configure('small.TLabel', background=light_grey, font=('Quicksand', 10, 'roman'))
    s.configure('tiny.TLabel', background=light_grey, font=('Quicksand', 8, 'roman'))

    s.configure('hyper.TLabel', background=light_grey, foreground=blue, cursor='hand2', font=('Quicksand', 10, 'roman'))

    s.configure('dark.TFrame', background=dark_purple)
    s.configure('light.TFrame', background=light_purple)
    s.configure('dark.TLabel', background=dark_purple, font=('Quicksand', 12, 'roman'))
    s.configure('light.TLabel', background=light_purple, font=('Quicksand', 12, 'roman'))


class Main(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent

        # general
        # self.parent.resizable(width=True, height=True)
        # self.parent.geometry("800x600")
        # self.parent.configure(bg=light_grey)


class MainApplication(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)

        # self.s = None
        # init_styles()
        self.parent = parent
        init_styles()

        # self.statusbar = Statusbar(self, parent)
        # self.toolbar = Toolbar(self)
        self.main = Main(self)

        # self.statusbar.pack(side="bottom", fill="x")
        # self.toolbar.pack(side="top", fill="x")
        self.main.pack(side="right", fill="both", expand=True)


if __name__ == "__main__":
    root = tk.Tk()
    root.geometry('1000x500')
    root.configure(bg=light_grey)
    MainApplication(root).pack(side="top", fill="both", expand=True)
    root.mainloop()

"""
if __name__ == "__main__":
    #root = tk.Tk()
    #root.geometry('1000x500')
    #root.configure(bg = light_grey)
    #MainApplication(root).pack(side="top", fill="both", expand=True)
    #root.mainloop()
    root = tk.Tk()
    root.title('Colored Navbar Test')
    #root.iconbitmap(default = 'icon.ico')
    root.geometry('1000x500')
    root.configure(bg = light_grey)
    #MainApplication(root).grid(row = 0, column = 0, sticky = (N, E, S, W))
    MainApplication(root).grid(row = 0, column = 0, sticky = (N, E, S, W))
    root.mainloop()

class Toolbar(tk.Frame):
    def __init__(self, parent, *args, **kwargs):
        tk.Frame.__init__(self, parent, *args, **kwargs)
        self.parent = parent

        # create a menu
        menu = tk.Menu(self.parent)
        self.parent.config(menu=menu)

        filemenu = tk.Menu(menu)
        menu.add_cascade(label="File", menu=filemenu)
        # filemenu.add_command(label="New", command=self.restart)
        # filemenu.add_command(label="Open...", command=self.open_file)
        filemenu.add_command(label="Save...")
        filemenu.add_separator()
        filemenu.add_command(label="Import...")
        filemenu.add_command(label="Export...")
        filemenu.add_separator()
        filemenu.add_command(label="Exit")

        viewmenu = tk.Menu(menu)
        menu.add_cascade(label="View", menu=viewmenu)
        viewmenu.add_command(label="View experiment")
        viewmenu.add_command(label="View history")

        helpmenu = tk.Menu(menu)
        menu.add_cascade(label="Help", menu=helpmenu)
        helpmenu.add_command(label="About...")
"""