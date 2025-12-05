import re
from playwright.sync_api import Playwright, sync_playwright, expect
import time

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    page.goto("http://localhost:9527/#/ohms-auto-test/index")
    time.sleep(2)
    page.goto("http://localhost:9527/#/login?redirect=%2Fohms-auto-test%2Findex")
    time.sleep(2)
    page.get_by_role("button", name="登陆").click()
    time.sleep(2)
    page.get_by_role("link", name="维护系统功能配置").click()
    time.sleep(2)
    page.get_by_text("发动机监控", exact=True).click()
    time.sleep(2)
    page.locator("form div").filter(has_text="需求信息 REQ-007 - 测试345 REQ-005").locator("i").first.click()
    time.sleep(2)
    page.get_by_placeholder("多选需求并添加").click()
    time.sleep(2)
    page.locator("li").filter(has_text="REQ-002 - 通信稳定性").click()
    time.sleep(2)
    page.get_by_role("button", name=" 保存配置").click()
    time.sleep(2)
    page.get_by_role("menu").get_by_role("link", name="维护系统自动测试").click()
    time.sleep(2)
    page.get_by_role("treeitem", name="  状态监控脚本").locator("div").first.click()
    time.sleep(2)
    page.get_by_role("button", name="").first.click(click_count=3)
    page.get_by_role("button", name="").click()
    page.get_by_role("button", name="关闭").click()

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
