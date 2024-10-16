import tkinter as tk

# 事件处理函数
def on_button_click():
    print("Button clicked!")

# 创建窗口
root = tk.Tk()
root.title("My Window")
root.geometry("300x200")

# 创建按钮组件并绑定单击事件
button = tk.Button(root, text="Click me")
button.bind("<Button-1>", lambda event: on_button_click())
button.pack()

# 运行窗口
root.mainloop()