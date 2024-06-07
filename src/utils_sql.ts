export function createPlaceholders(count: number, offset = 0) {
  return Array.from(new Array(count).keys()).map((index) => {
    return `$${index + 1 + offset}`
  })
}

export function prepareForSQL(data: Record<string, unknown>) {
  const columns: Array<string> = Object.keys(data);
  return {
    columns: columns.join(','),
    placeholders: createPlaceholders(columns.length).join(','),
    values: columns.map((name) => data[name]),
  }
}