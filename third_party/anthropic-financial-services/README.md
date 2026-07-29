# third_party / anthropic-financial-services

미국주식 다이제스트가 쓰는 **Anthropic 공식 스킬 1건**의 출처·라이선스 기록.

| 항목 | 값 |
|---|---|
| 원본 | https://github.com/anthropics/financial-services |
| 커밋 | `eb0c1ea962d4c6cee07f4920e36b1aa7a025d320` (2026-07-22) |
| 가져온 날 | 2026-07-29 |
| 라이선스 | Apache-2.0 (전문: [`LICENSE`](LICENSE) — 원본 파일 그대로, 저작권자 표기 무수정) |

## 가져온 것

| 경로(원본) | 이 레포 | 상태 |
|---|---|---|
| `plugins/vertical-plugins/equity-research/skills/earnings-preview/SKILL.md` | `skills/earnings-preview/SKILL.md` | **무수정(unmodified)** |

- **`commands/` 는 가져오지 않았다** — "이 스킬로 모닝노트를 써라" 식의 맨 지시문이라 우리 프롬프트에
  섞이면 지시 오염이 된다. 스킬 본문만 필요하다.
- **플러그인 설치도 하지 않는다** — `equity-research/plugin.json` 에 스킬 목록이 없고 `hooks.json` 은
  빈 껍데기라 디렉터리 관례로만 로드된다. `SKILL.md` 파일 하나면 충분하다(부속 파일·실행 코드 0).

## 어떻게 쓰이나

`us_digest.prepare_skill()` 이 이 파일을 **다이제스트 전용 샌드박스**
(`%TEMP%/claude_bridge_us_digest_sandbox/.claude/skills/earnings-preview/`)로 복사한다.
스킬 탐색은 **cwd 기준**이라 그 호출에만 걸리고 개발자의 다른 claude 세션에는 딸려가지 않는다.
도구는 `Skill` 하나만 열린다(ADR-004) — 파일·셸·git·네트워크 도구는 여전히 0개다.

## 왜 `SKILL.md` 안에 고지 주석을 넣지 않았나

처음엔 파일 상단에 출처 주석을 넣었다가 **되돌렸다**. 두 가지 이유 모두 실질적이다.

1. **frontmatter 가 깨진다** — Claude Code 는 파일 **첫 줄부터**의 `---` YAML 블록에서
   `name`/`description` 을 읽는다. 그 앞에 HTML 주석이 오면 스킬이 로드 안 될 수 있다.
2. **고지 자체가 거짓이 된다** — 주석을 넣는 순간 그 파일은 더 이상 "unmodified" 가 아니다.

그래서 파일은 **바이트 단위로 원본과 동일**하게 두고(sha256 은 [`NOTICE`](NOTICE) 에 기록),
귀속·라이선스는 `LICENSE`·`NOTICE`·이 문서로 남긴다. Apache-2.0 §4(b) 의 변경 고지 의무는
**수정한 파일에만** 걸리므로 이 구성으로 충족된다.

## 상위 디렉터리에 무엇을 더 넣을 때

- 파일을 **수정하면** 그때는 Apache-2.0 §4(b) 변경 고지를 남긴다 — frontmatter 가 있는 파일이면
  주석을 frontmatter **뒤**에 두고, `NOTICE` 의 "unmodified" 표기도 함께 고친다.
- 위 표에 원본 경로·커밋·수정 여부를 함께 적는다. **거짓 고지 금지.**
