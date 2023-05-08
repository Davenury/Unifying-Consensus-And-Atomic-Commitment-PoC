import puppeteer from "puppeteer";
import path from 'path';
import {fileURLToPath} from 'url';

const getBaseDownloadPath = () => {
    const __filename = fileURLToPath(import.meta.url)
    return `${path.dirname(__filename)}/../data-processing`
}

const allPanels = [14, 18, 27, 4, 12, 21]
// const panels = [14, 18, 27, 4, 12, 31, 21]
const panelsWithoutChanges = [14, 18, 27, 4, 21]
const panelWithChangeSynchronization = [14, 18, 27, 4, 12, 33, 21]
const leaderPanel = [31]

const baseDownloadPath = process.env.BASE_DOWNLOAD_PATH ?? getBaseDownloadPath()
const namespace = process.env.NAMESPACE ?? "ddebowski"
const from = process.env.START_TIMESTAMP ?? "1683454588474"
const to = process.env.END_TIMESTAMP ?? "1683454599021"
const protocol = process.env.PROTOCOL ?? "alvin"
const experiment = process.env.EXPERIMENT ?? "3x1"
const scrapingType = process.env.SCRAPING_TYPE ?? "all"

let panels = []
if (scrapingType === "all") panels = allPanels
else if (scrapingType === "synchronization") panels = panelWithChangeSynchronization
else if (scrapingType === "leader") panels = leaderPanel
else if (scrapingType === "without-changes") panels = panelsWithoutChanges


const experiments = [
    {
        namespace: namespace,
        from: from,
        to: to,
        protocol: protocol,
        experiment: experiment,
        scrapingType: scrapingType,
        panels: panels
    }
]

console.log(experiments)

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
        downloadPath: `${baseDownloadPath}/${protocol}/${experiment}`
    });
    await page.goto(`http://localhost:3000/d/HSUzSq2Vk/poc?orgId=1&refresh=10s&&viewPanel=${panelId}&inspect=${panelId}&var-namespace=${namespace}&from=${from}&to=${to}`, {waitUntil: "load"});

    console.log("PanelId:",panelId)

    await page.waitForSelector("div[role='dialog']")
    await new Promise(resolve => setTimeout(resolve, 1_000));
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
    return new Promise(function (resolve) {
        setTimeout(resolve, time)
    });
}

(async () => {
    const browser = await puppeteer.launch({headless: false});
    const page = await browser.newPage();
    await page.setViewport({width: 1366, height: 768});

    for (let experiment of experiments) {
        for (let panel of panels) {
            await downloadFile(page, {...experiment, panelId: panel})
            await delay(1000)
        }
    }

    await browser.close()
})()
