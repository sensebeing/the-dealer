/** YAML data subset used by the team registry and canonical requests. No tags, aliases or executable values. */
function splitFlow(text: string, delimiter: string): string[] {
  const parts: string[] = [];
  let quote = '';
  let depth = 0;
  let start = 0;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quote) {
      if (quote === '"' && char === '\\') i++;
      else if (char === quote) {
        if (quote === "'" && text[i + 1] === "'") i++;
        else quote = '';
      }
    } else if (char === '"' || char === "'") quote = char;
    else if (char === '[' || char === '{') depth++;
    else if (char === ']' || char === '}') depth--;
    else if (depth === 0 && char === delimiter) {
      parts.push(text.slice(start, i));
      start = i + 1;
    }
  }
  if (quote || depth !== 0) throw new Error('Unclosed YAML flow value');
  parts.push(text.slice(start));
  return parts;
}

function stripComment(text: string): string {
  let quote = '';
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (quote) {
      if (quote === '"' && char === '\\') i++;
      else if (char === quote) {
        if (quote === "'" && text[i + 1] === "'") i++;
        else quote = '';
      }
    } else if (char === '"' || char === "'") quote = char;
    else if (char === '#' && (i === 0 || /\s/.test(text[i - 1]))) return text.slice(0, i).trimEnd();
  }
  return text.trimEnd();
}

function entry(text: string): [string, string] {
  const match = /^([^:]+):(?:\s+(.*)|$)/.exec(text);
  if (!match) throw new Error(`Unsupported YAML mapping: ${text}`);
  const key = scalar(match[1].trim());
  if (typeof key !== 'string' || ['__proto__', 'constructor', 'prototype', '<<'].includes(key)) {
    throw new Error('Unsafe or unsupported YAML key');
  }
  return [key, match[2] ?? ''];
}

function put(target: Record<string, any>, key: string, value: any): void {
  if (Object.hasOwn(target, key)) throw new Error(`Duplicate YAML key: ${key}`);
  target[key] = value;
}

function scalar(raw: string): any {
  const text = raw.trim();
  if (text.startsWith('"')) return JSON.parse(text);
  if (text.startsWith("'")) {
    if (!text.endsWith("'") || text.length < 2) throw new Error('Unclosed YAML quote');
    return text.slice(1, -1).replaceAll("''", "'");
  }
  if (text.startsWith('[') && text.endsWith(']')) {
    const body = text.slice(1, -1).trim();
    return body ? splitFlow(body, ',').map(scalar) : [];
  }
  if (text.startsWith('{') && text.endsWith('}')) {
    const result = {};
    const body = text.slice(1, -1).trim();
    for (const item of body ? splitFlow(body, ',') : []) {
      const [key, value] = entry(item.trim());
      put(result, key, scalar(value));
    }
    return result;
  }
  if (/^[&*!\[\]{}]/.test(text)) throw new Error(`Unsupported YAML value: ${text}`);
  if (text === 'null' || text === '~') return null;
  if (text === 'true' || text === 'false') return text === 'true';
  if (/^-?(0|[1-9]\d*)(\.\d+)?$/.test(text)) return Number(text);
  return text;
}

export function parseYaml(text: string): Record<string, any> {
  const lines = text.replace(/^\uFEFF/, '').split(/\r?\n/);
  let index = 0;
  const indent = (line: string) => line.length - line.trimStart().length;
  const skip = () => {
    while (index < lines.length && !stripComment(lines[index]).trim()) index++;
  };
  const value = (raw: string, parentIndent: number): any => {
    if (/^[>|][-+]?$/.test(raw)) {
      const content: string[] = [];
      let width: number | undefined;
      while (index < lines.length) {
        const line = lines[index];
        if (line.trim() && indent(line) <= parentIndent) break;
        if (line.trim()) width ??= indent(line);
        content.push(line.slice(width ?? line.length));
        index++;
      }
      while (content.at(-1) === '') content.pop();
      const body = raw[0] === '>' ? content.join('\n').replace(/([^\n])\n(?=[^\n])/g, '$1 ') : content.join('\n');
      return body + (raw.endsWith('-') ? '' : '\n');
    }
    if (raw) {
      let text = raw;
      while (index < lines.length && lines[index].trim() && indent(lines[index]) > parentIndent) {
        text += ` ${stripComment(lines[index++].trim())}`;
      }
      return scalar(text);
    }
    skip();
    if (index >= lines.length) return null;
    const childIndent = indent(lines[index]);
    const indentlessList = childIndent === parentIndent && /^-(\s|$)/.test(lines[index].trimStart());
    return childIndent > parentIndent || indentlessList ? block(childIndent) : null;
  };
  const mapping = (target: Record<string, any>, raw: string, level: number) => {
    const [key, rest] = entry(raw);
    put(target, key, value(rest, level));
  };
  const block = (level: number): any => {
    skip();
    const sequence = /^-(\s|$)/.test(lines[index].trimStart());
    const result: any = sequence ? [] : {};
    while (index < lines.length) {
      skip();
      if (index >= lines.length || indent(lines[index]) < level) break;
      if (sequence && indent(lines[index]) === level && !/^-(\s|$)/.test(lines[index].trimStart())) break;
      if (/^ *\t/.test(lines[index]) || indent(lines[index]) !== level) throw new Error(`Invalid YAML indentation at line ${index + 1}`);
      const raw = stripComment(lines[index++].slice(level));
      if (sequence) {
        if (!/^-(\s|$)/.test(raw)) throw new Error('Mixed YAML sequence and mapping');
        const rest = raw.slice(1).trimStart();
        if (!/^[\[{'"]/.test(rest) && /^[^:]+:(\s|$)/.test(rest)) {
          const item = {};
          mapping(item, rest, level + 2);
          skip();
          if (index < lines.length && indent(lines[index]) > level) {
            const tail = block(level + 2);
            if (Array.isArray(tail)) throw new Error('Invalid YAML sequence mapping');
            for (const [key, val] of Object.entries(tail)) put(item, key, val);
          }
          result.push(item);
        } else result.push(value(rest, level));
      } else mapping(result, raw, level);
    }
    return result;
  };
  skip();
  if (index === lines.length) return {};
  const result = block(0);
  if (Array.isArray(result)) throw new Error('YAML document must be a mapping');
  return result;
}

export function parseMarkdownFrontmatter(markdownText: string): { frontmatter: Record<string, any>; body: string } {
  const text = markdownText.replace(/^\uFEFF/, '');
  if (!/^---\r?\n/.test(text)) return { frontmatter: {}, body: markdownText };
  const match = /^---\r?\n([\s\S]*?)^---[ \t]*(?:\r?\n|$)/m.exec(text);
  if (!match) throw new Error('Unclosed Markdown frontmatter');
  return { frontmatter: parseYaml(match[1]), body: text.slice(match[0].length) };
}
