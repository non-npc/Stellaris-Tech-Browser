from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Tuple


@dataclass
class Token:
    kind: str
    value: str
    position: int


class ClausewitzParserError(Exception):
    pass


class ClausewitzTokenizer:
    def __init__(self, text: str) -> None:
        self.text = text
        self.length = len(text)
        self.pos = 0

    def tokenize(self) -> List[Token]:
        tokens: List[Token] = []
        while self.pos < self.length:
            ch = self.text[self.pos]
            if ch.isspace():
                self.pos += 1
                continue
            if ch == '#':
                self._skip_comment()
                continue
            if ch in '{}=':
                tokens.append(Token(ch, ch, self.pos))
                self.pos += 1
                continue
            if ch == '"':
                tokens.append(Token('STRING', self._read_string(), self.pos))
                continue
            tokens.append(Token('ATOM', self._read_atom(), self.pos))
        return tokens

    def _skip_comment(self) -> None:
        while self.pos < self.length and self.text[self.pos] != '\n':
            self.pos += 1

    def _read_string(self) -> str:
        self.pos += 1
        start = self.pos
        result: List[str] = []
        while self.pos < self.length:
            ch = self.text[self.pos]
            if ch == '\\' and self.pos + 1 < self.length:
                result.append(self.text[self.pos + 1])
                self.pos += 2
                continue
            if ch == '"':
                value = ''.join(result)
                self.pos += 1
                return value
            result.append(ch)
            self.pos += 1
        raise ClausewitzParserError(f'Unterminated string starting at {start}')

    def _read_atom(self) -> str:
        start = self.pos
        while self.pos < self.length:
            ch = self.text[self.pos]
            if ch.isspace() or ch in '{}=#':
                break
            self.pos += 1
        return self.text[start:self.pos]


class ClausewitzParser:
    def parse_file(self, path: Path) -> dict[str, Any]:
        text = path.read_text(encoding='utf-8-sig', errors='replace')
        return self.parse_text(text)

    def parse_text(self, text: str) -> dict[str, Any]:
        tokens = ClausewitzTokenizer(text).tokenize()
        parser = _TokenParser(tokens)
        return parser.parse_root()


class _TokenParser:
    def __init__(self, tokens: List[Token]) -> None:
        self.tokens = tokens
        self.pos = 0

    def parse_root(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        while not self._eof():
            key = self._expect_any(['ATOM', 'STRING']).value
            if self._match('='):
                value = self._parse_value()
                self._append_value(result, key, value)
            else:
                self._append_value(result, key, True)
        return result

    def _parse_value(self) -> Any:
        if self._match('{'):
            return self._parse_brace_block()
        token = self._expect_any(['ATOM', 'STRING'])
        return token.value

    def _parse_brace_block(self) -> Any:
        items: List[Tuple[str, Any]] = []
        values: List[Any] = []
        saw_assignment = False

        while not self._eof() and not self._peek_is('}'):
            if self._peek_kind() not in ('ATOM', 'STRING'):
                raise ClausewitzParserError(f'Unexpected token {self._peek_kind()} at position {self._peek().position}')
            token = self._advance()
            if self._match('='):
                saw_assignment = True
                value = self._parse_value()
                items.append((token.value, value))
            else:
                values.append(token.value)
        self._expect('}')

        if saw_assignment:
            result: dict[str, Any] = {}
            for key, value in items:
                self._append_value(result, key, value)
            if values:
                existing = result.get('__items__', [])
                if not isinstance(existing, list):
                    existing = [existing]
                existing.extend(values)
                result['__items__'] = existing
            return result
        return values

    def _append_value(self, mapping: dict[str, Any], key: str, value: Any) -> None:
        if key in mapping:
            current = mapping[key]
            if isinstance(current, list):
                current.append(value)
            else:
                mapping[key] = [current, value]
        else:
            mapping[key] = value

    def _peek(self) -> Token:
        return self.tokens[self.pos]

    def _peek_kind(self) -> str:
        return self.tokens[self.pos].kind

    def _peek_is(self, kind: str) -> bool:
        return not self._eof() and self.tokens[self.pos].kind == kind

    def _advance(self) -> Token:
        token = self.tokens[self.pos]
        self.pos += 1
        return token

    def _match(self, kind: str) -> bool:
        if self._peek_is(kind):
            self.pos += 1
            return True
        return False

    def _expect(self, kind: str) -> Token:
        if not self._peek_is(kind):
            raise ClausewitzParserError(f'Expected {kind}, got {self._peek_kind()} at position {self._peek().position}')
        return self._advance()

    def _expect_any(self, kinds: List[str]) -> Token:
        if self._eof() or self._peek_kind() not in kinds:
            raise ClausewitzParserError(f'Expected one of {kinds}, got {self._peek_kind() if not self._eof() else "EOF"}')
        return self._advance()

    def _eof(self) -> bool:
        return self.pos >= len(self.tokens)
