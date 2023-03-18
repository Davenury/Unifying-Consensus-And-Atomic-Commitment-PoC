import puppeteer from "puppeteer";

const experiments = [
    {
        namespace: "ddebowski",
        from: "1679148002592",
        to: "1679148130702",
        protocol: "gpac",
        experiment: "3x2",
    }
]

const panels = [14, 18, 27, 4, 12]
const downloadFile = async (page, {
    namespace,
    panelId,
    from,
    to,
    protocol,
    experiment
}) => {
    const client = await page.target().createCDPSession()
    await client.send('Page.setDownloadBehavior', {
        behavior: 'allow',
        downloadPath: `/home/dawid/workspace/studia/magisterka/results/20-03/${protocol}/${experiment}`
    });
    await page.goto(`http://localhost:3000/d/HSUzSq2Vk/poc?orgId=1&refresh=10s&&viewPanel=${panelId}&inspect=${panelId}&var-namespace=${namespace}&from=${from}&to=${to}`, {waitUntil: "load"});

    await page.waitForSelector("div[role='dialog']")
    const dataOptionsButton = await page.waitForSelector('button[aria-controls="Data options"]')
    await dataOptionsButton.click()

    await page.evaluate(() => {
        document.querySelector("#formatted-data-toggle").click();
    });

    if (await page.$('input[type="text"]')) {
        await page.type('input[type="text"]', "Series joined by time")
        await page.keyboard.press("Enter")
    }

    await page.waitForSelector('span[class="css-1mhnkuh"]')
    const button = await page.waitForSelector('button[class="css-1vp08vr-button"]')
    await page.evaluate((el) => {
        el.click()
    }, button)
}

function delay(time) {
    return new Promise(function(resolve) {
        setTimeout(resolve, time)
    });
}

(async () => {
    const browser = await puppeteer.launch({headless: false});
    const page = await browser.newPage();
    await page.setViewport({ width: 1366, height: 768});

    for (let experiment of experiments) {
        for (let panel of panels) {
            console.log(`panelId: ${panel}`)
            await downloadFile(page, {...experiment, panelId: panel})
            await delay(1000)
        }
    }

    await browser.close()
})()
