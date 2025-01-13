import { input } from "@inquirer/prompts";
import chalk from "chalk";
import fs from "fs";
import path from "path";

const DEMO_REPO_API = 'https://api.github.com/repos/dbos-inc/dbos-demo-apps';
const TS_DEMO_PATH = 'typescript/';
const BRANCH = 'main';
export const IGNORE_PATTERNS = ['.dbos/', 'node_modules/', 'dist/', '.git/', 'venv/', '.venv/', '.direnv/', '.devenv/'];
export const IGNORE_REGEX = new RegExp(`^.*\\/(${IGNORE_PATTERNS.join('|')}).*$`);
let GITHUB_TOKEN = process.env.GITHUB_TOKEN;

type GitHubTree = {
  sha: string;
  url: string;
  tree: GitHubTreeItem[];
  truncated: boolean;
};

type GitHubTreeItem = {
  path: string;
  mode: string;
  type: string;
  sha: string;
  url: string;
  size: number;
};

type GitHubItem = {
  sha: string;
  node_id: string;
  url: string;
  content: string;
  encoding: string;
  size: number;
};

export async function createTemplateFromGitHub(appName: string, templateName: string) {
  console.log(`Creating a new application named ${chalk.bold(appName)} from the template ${chalk.bold(templateName)}`);
  // Prompt user to pptionally provide a GitHub personal access token
  if (!GITHUB_TOKEN) {
    GITHUB_TOKEN = await input(
      {
        message: '(Optional) Enter your GitHub personal access token to increase the rate limit for downloading the template',
        default: '',
      });
  }

  const tree = await fetchGitHubTree(BRANCH);

  const templatePath = `${TS_DEMO_PATH}${templateName}/`;
  const filesToDownload = tree.filter((item) => item.path.startsWith(templatePath) && item.type === 'blob');

  // Download every file from the template
  await Promise.all(
    filesToDownload
      .filter((item) => !IGNORE_REGEX.test(item.path))
      .map(async (item) => {
        const rawContent = await fetchGitHubItem(item.url);
        const content = Buffer.from(rawContent, 'binary').toString('utf-8');
        const filePath = item.path.replace(templatePath, '');
        const targetPath = `${appName}/${filePath}`;
        await fs.promises.mkdir(path.dirname(targetPath), { recursive: true });
        await fs.promises.writeFile(targetPath, content, {mode: parseInt(item.mode, 8)});
      })
  );
  console.log(`Downloaded ${filesToDownload.length} files from the template GitHub repository`);
}

async function fetchGitHub(url: string): Promise<Response> {
  const requestInit: RequestInit = {};
  if (GITHUB_TOKEN) {
    requestInit.headers = {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
    };
  }
  const response = await fetch(url, requestInit);
  if (!response.ok) {
    if (response.headers.get('x-ratelimit-remaining') === '0') {
      throw new Error(
        `Error fetching from GitHub API: rate limit exceeded.\r\nPlease wait a few minutes and try again.\r\nTo increase the limit, you can create a personal access token and set it in the ${chalk.bold('GITHUB_TOKEN')} environment variable. \r\nDetails: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api`,
      );
    } else if (response.status === 401) {
      throw new Error(`Error fetching content from GitHub ${url}: ${response.status} ${response.statusText}.\r\nPlease ensure your ${chalk.bold('GITHUB_TOKEN')} environment variable is set to a valid personal access token.`);
    }
    throw new Error(`Error fetching content from GitHub ${url}: ${response.status} ${response.statusText}`);
  }

  return response;
}

async function fetchGitHubTree(tag: string) {
  const response = await fetchGitHub(`${DEMO_REPO_API}/git/trees/${tag}?recursive=1`);
  const { tree } = (await response.json()) as GitHubTree;
  return tree;
}

async function fetchGitHubItem(url: string) {
  const response = await fetchGitHub(url);
  const { content: contentBase64 } = (await response.json()) as GitHubItem;
  return atob(contentBase64); // Decode base64
}
