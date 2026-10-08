# claude-bridge

**디스코드에서 보낸 한 줄로 Claude Code 를 원격 실행하는 디스코드 실행비서.**
PC 앞에 없어도 디스코드에 지시를 보내면 상시 호스트가 대신 작업하고,
커밋은 `push` 로 승인했을 때만 원격에 올린다. 봇 프레임워크 없이 **플랫폼 무관 코어 +
어댑터 계층**으로 설계해, 메신저·호스트를 갈아끼울 수 있는 구조를 유지한다(현재 구현: 디스코드).

![Python](https://img.shields.io/badge/Python-3.13-3776AB?logo=python&logoColor=white)
![discord.py](https://img.shields.io/badge/discord.py-2.7-5865F2?logo=discord&logoColor=white)
![core](https://img.shields.io/badge/core-stdlib_only-success)
![tests](https://img.shields.io/badge/tests-1085_passing-success)

<!-- 스크린샷: (추후) — 현재는 아래 아키텍처 다이어그램으로 대체 -->

## 아키텍처 — 플랫폼 무관 코어 + 어댑터

```mermaid
graph TB
  subgraph adapters["플랫폼 어댑터 (교체 가능한 seam)"]
    dc["DiscordAdapter<br/>Gateway WebSocket · discord.py"]
  end
  subgraph core["플랫폼 무관 코어 (stdlib)"]
    disp["handle_event(adapter, event)<br/>인가 게이트 · 라우팅"]
    run["claude 러너 · git · 스케줄 · 순수 파서"]
  end
  dc -->|정규화 Event| disp
  disp -->|send/edit/ack via adapter| dc
  disp --> run
  run -->|stdin 주입 · headless| claude["claude -p"]
  run -.->|push 승인 시 pull --rebase → push| git[("대상 레포")]
```

코어는 **`Adapter` 계약**(poll·send·edit·ack·fetch_file·close + 채널 헬퍼)과 2개 정규화
dataclass(`Event`·`Button`)만 안다. 디스코드 이벤트는 어댑터가 `Event` 로 정규화하고, 코어의
`Button` 리스트를 플랫폼 UI(discord View)로 렌더한다. 이 인터페이스가 **플랫폼 교체 seam** —
다른 메신저로 바꾸려면 어댑터 1개만 새로 쓰면 되고 코어는 그대로다. **외부 의존성은 `discord.py`
(+ 음성 재생용 `PyNaCl`·`davey`·`yt-dlp`·`imageio-ffmpeg`)뿐이며 `discord_adapter.py` 한 파일에만
격리**된다 — 코어(`bridge.py`·`adapter.py`)는 표준 라이브러리 전용이라, 어댑터를 지연 import 하는
경로(단위 테스트)는 discord.py 없이도 돈다.

디스코드 수신은 상시 WebSocket(Gateway)이고, 코어는 **단일 워커의 직렬 루프**다. 어댑터가
asyncio(이벤트루프 스레드) ↔ 동기 코어(워커 스레드)를 큐 + `run_coroutine_threadsafe` 로 이어
"한 번에 하나" 불변식을 지킨다. 인바운드 포트를 열지 않는다(전부 아웃바운드) — 상시 호스트의
공격면을 최소화한다.

## 주요 기능

| 기능 | 설명 |
|---|---|
| **채널 = 프로젝트** | 봇이 프로젝트별 채널을 자동 생성. `#etf-info` 채널에서 프로젝트명 없이 지시만 보내면 그 프로젝트에서 실행 |
| **상태색 임베드** | 완료 초록 · 실패 빨강 · 진행 노랑 · 확인대기 블러플. 진행→완료는 같은 메시지를 편집(채널이 안 쌓임) |
| **버튼 UI** | 프로젝트 선택·`push` 승인을 탭 버튼으로 — 폰 타이핑 최소화 |
| **한글 명령** | `ㅁ프로젝트`·`ㅁ도움말`·`ㅁ취소`·`ㅁ재시작`·`ㅁ새대화` — 모든 명령은 `ㅁ` 접두 |
| **승인형 push** | 로컬 커밋까지만 자동, 원격 반영은 `ㅁ푸시해줘`(또는 버튼)로 명시 |
| **`ㅁ재시작`** | 봇이 `🔴 bridge Off` 를 남기고 종료 → 감독자(`run_loop.ps1`)가 몇 초 안에 다시 띄워 `🟢 bridge On` |
| **예약 알림** | 지정 시각에 브리지가 `[시각] ⏰ <스케쥴> 제목` 한 줄로 알림(해당 프로젝트 채널, 없으면 `#알림`) |
| **시스템 소식** | `#알림` 채널에 `[시각]` 머리로 기동(🟢 On) · 10분 이상 끊겼다 복구(🔴 Off → 🟢 On) · Claude 로그인 만료(🔐) · 다이제스트 포기(⛔)를 알린다 |
| **마이크론 다이제스트** | `#마이크론` 에 평일 21:30 시세·공시·뉴스, 일요일 21:30 실적·재무(SEC)·증권사 시각·투자자 분위기를 마크다운 메시지로. 용어마다 쉬운 풀이(`-# 💬`) |
| **사진 + 지시** | 캡처를 캡션(지시)과 함께 보내면 이미지 경로를 주입해 그대로 작업 실행(멀티모달 수신, CDN 도메인 고정) |
| **음악 재생** | 음성채널에서 유튜브 재생목록을 셔플 재생(`ㅁ노래`·`ㅁ정지`·`ㅁ다음`) + 곡 지정 재생 `ㅁ재생 <제목>` · 목록 조회/추가/제거 `ㅁ목록`/`ㅁ추가`/`ㅁ삭제`. 곡이 바뀔 때마다 채널에 제목을 알린다 |
| **차트 일괄 담기** | `ㅁ스포티파이` 한 번이면 스포티파이 주간차트(글로벌·일본·한국, kworb 미러) 상위 30곡씩 90곡을 재생목록에 담고 「✅처리완료 / 추가 N곡 / 중복 N곡 / 실패 N곡」 4줄로 알린다(약 7분 소요 — 「🎧 스포티파이 월간차트 추가 / 차트를 가져오고 있습니다(7분 예상)」 2줄로 먼저 알린다). **한 달에 한 번은 자동** — 매일 09:00~23:59 에 한 번 확인해 이번 달(`YYYY-MM` 스탬프) 미완료면 담는다. 1일에 브리지가 꺼져 있으면 처음 켜진 날 담는다 |
| **검색 결과 고르기** | `ㅁ추가 <검색어>` 는 후보 5건 중 방송무대·교차편집·직캠을 걸러 고른다. 다른 것을 원하면 `ㅁ추가 <검색어> #2`(순번은 **1~99**, 그 밖의 `#0`·`#100` 은 검색어의 일부로 본다) |

## Why — 왜 만들었나

**문제**: 외출 중에도 "이 버그 고쳐줘" 한 줄을 처리하고 싶다. 그런데 원격 코드 실행·커밋은
그 자체로 위험한 표면이고, 개인 기기는 공개 엔드포인트가 없다.

**해결**: 봇 프레임워크 없이 **어댑터 패턴**으로 플랫폼 무관 코어를 짜서, 메신저와 호스트 PC 를
자유롭게 갈아끼울 수 있게 했다. 원격 실행 표면은 신원 게이트·인젝션 방어·최소
권한으로 다층 방어하고, **인바운드 포트 0**(아웃바운드 전용)으로 노출면을 최소화한다. 개인 원격
도구에 맞춰 **운영 표면을 최소화**하는 것이 최우선 가치다.

## 보안 설계 (기술적 트레이드오프)

원격에서 코드를 실행·커밋하는 표면이라 방어를 다층으로 둔다. 결정 근거는 의사결정 기록(ADR)에 남겨 두었다.

| 경계 | 방어 | 트레이드오프 |
|---|---|---|
| **신원 게이트** | 허용 유저 ID(`DISCORD_ALLOWED_USER_IDS`) 목록 필수 — 밖은 무회신·로그만. 빈 목록이면 기동 거부 | 봇은 공개 검색·초대될 수 있음 → 허용목록이 유일 방벽. 1인 비공개 서버 운용 권장 |
| **명령 인젝션(RCE)** | 사용자 입력을 argv 에 두지 않고 **stdin 으로만** claude 에 전달 | Windows `claude.CMD` shim 의 `cmd.exe` 재파싱을 stdin 전용으로 원천 차단 |
| **최소 권한** | 경로별 도구 티어 — 작업·사진(편집+git 커밋)·다이제스트(0개, 실적 발표 창에서만 `Skill` 1개). 일반 셸·`git push` 는 **어느 티어에도 없음** | 사진은 "이 캡처 보고 고쳐줘"가 실사용이라 작업과 같은 도구셋이다 — 이미지 속 악성 텍스트(confused-deputy)는 프롬프트 인젝션 가드로 억제하되, 실효 방어는 **`git push` 미부여 + 사용자 승인 push**(악성 커밋도 로컬에 머문다) |
| **파일 다운로드** | 사진 URL 도메인 고정(디스코드 CDN 화이트리스트)·확장자·10MB·경로 트래버설 차단·리다이렉트 미추종 | 임의 URL 다운로드·내부망(SSRF) 접근 차단 |
| **비밀값** | 봇 토큰·인가키는 `.env` 로만(커밋 금지). 회신·로그에서 토큰·내부 경로 마스킹 | — |
| **푸시 통제** | 로컬 커밋까지만 자동, 원격 반영은 사용자 승인 시 `pull --rebase` 후 | claude 에는 push 권한 없음 |
| **단일 인스턴스** | pidfile 락 — 같은 봇을 두 곳에서 구동하면 Gateway 세션 충돌 | 브리지는 한 호스트에서만 실행 |

## Quick Start

```bash
git clone <repo> && cd claude-bridge
python -m pip install -r requirements.txt   # discord.py + 음성 재생 의존성
cp .env.example .env                        # 봇 토큰·인가키 채우기
python bridge.py                            # 단발 실행. 상시 운영은 start.ps1(감독자 run_loop.ps1)
```

`.env` 에 `DISCORD_BOT_TOKEN` 과 `DISCORD_ALLOWED_USER_IDS` 를 채운 뒤 실행한다.
디스코드에서 `ㅁ도움말` 로 시작.

## 환경 변수 (`.env`)

`.env.example` 을 복사해 채운다. **실제 토큰·ID 는 커밋 금지** — 아래는 이름·형식만.

| 변수 | 설명 |
|---|---|
| `DISCORD_BOT_TOKEN` | 디스코드 봇 토큰 |
| `DISCORD_ALLOWED_USER_IDS` | 허용 유저 ID(콤마 구분) |
| `CLAUDE_TIMEOUT_SEC` | claude 작업 1건 최대 실행 시간(초, 기본 900) |
| `TARGET_ROOT` | 원격 지시 대상 프로젝트 루트(직속 폴더만) |
| `MUSIC_PLAYLIST_ID` | `ㅁ노래` 재생 · `ㅁ목록`/`ㅁ추가`/`ㅁ삭제`/`ㅁ스포티파이` 대상 유튜브 재생목록 **ID**(URL 의 `list=` 뒤). 비우면 `ㅁ노래` 는 미설정 안내 |
| `SEC_USER_AGENT` | 마이크론 다이제스트의 SEC 공시·재무 조회용 `<영문 이름> <이메일>`(SEC 요구 형식). 비우면 SEC 블록을 건너뛴다 |

## 호스팅

브리지는 상시 프로세스라 **켜 두는 동안만** 원격이 동작한다.

- **로컬(노트북)**: `start.ps1` 이 감독자 `run_loop.ps1` 을 숨김으로 띄우고, 감독자가 봇을 자식으로 돌린다 —
  `ㅁ재시작`·크래시·**코드 변경 자동 재시작** 뒤 몇 초 안에 다시 띄운다. 코드를 고치면 봇이 하던 일이
  끝난 뒤 스스로 껐다 켠다(`🟢` 기동 알림 생략). **끄는 법: `stop.ps1`**(그냥 죽이면 감독자가 되살린다).
  절전 해제 필수(잠자면 수신 중단). Gateway 는 아웃바운드라 공개 포트·포트포워딩이 필요 없다.

### 호스트 지정(마커)·로그인 자동 기동

같은 봇 토큰으로 두 PC 가 동시에 뜨면 Gateway 가 충돌하므로, `start.ps1` 은 프로젝트 루트에
`bridge_host.local` 파일이 있는 PC 에서만 브리지를 띄운다. 없으면 `NOT_HOST ...` 를 출력하고
아무것도 하지 않는다(`-Force` 로도 우회 불가). 마커는 `.gitignore` 대상이라 PC 마다 따로 만든다.

```powershell
New-Item bridge_host.local -ItemType File     # 이 PC 를 호스트로 지정
```

윈도우 로그인 시 자동 기동은 시작프로그램 폴더에 바로가기를 한 번 만들어 둔다(배치 파일·관리자 권한 불필요).
(`-NoPing` 은 옛 바로가기 호환용으로 남은 무동작 스위치다.)

```powershell
$d = (Get-Location).Path; $s = New-Object -ComObject WScript.Shell; $l = $s.CreateShortcut((Join-Path ([Environment]::GetFolderPath('Startup')) 'claude-bridge.lnk')); $l.TargetPath = 'powershell.exe'; $l.Arguments = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$d\start.ps1`" -NoPing"; $l.WorkingDirectory = $d; $l.WindowStyle = 7; $l.Save()
```

## 개발

```bash
python -m pip install -r requirements-dev.txt   # pytest·ruff·mypy(런타임과 분리)
python -m pytest             # 단위·계약 테스트(순수 함수·FakeAdapter, 네트워크 없음)
ruff check . && ruff format --check .
mypy .
# 순수 함수·보안 경계 검증도 위 pytest 에 들어 있다(별도 --selftest 없음)
```

코어(`bridge.py`)·계약과 공유 유틸(`adapter.py`)·어댑터(`discord_adapter.py`)로 분리돼 있고,
파싱·허용목록·경로 해석·마스킹·콜백 코덱 등 순수 함수와 어댑터 계약이 테스트로 고정된다.

## 문서

- **아키텍처** — 단일 루프 구조와 어댑터 경계
- **의사결정(ADR)** — 단일루프·stdin 인젝션 방어·경로별 도구 권한·stream-json
- **어댑터 계층** — 어댑터 설계·계약·UX
