export async function fetchWithRetry(
  input: string | URL | Request,
  init?: RequestInit,
  { retries = 2, delay = 500 } = {},
): Promise<Response> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const response = await fetch(input, init);
      if (response.status >= 500 && attempt < retries) {
        lastError = new Error(`Server error (${response.status})`);
        await new Promise((resolve) => setTimeout(resolve, delay * (attempt + 1)));
        continue;
      }
      return response;
    } catch (error) {
      lastError = error;
      if (attempt < retries) {
        await new Promise((resolve) => setTimeout(resolve, delay * (attempt + 1)));
        continue;
      }
    }
  }

  throw lastError;
}
