"""
Excel + MarketSpeed II RSS を xlwings で操作して 1分足OHLCV を取得。
無効行除外 → セッションVWAP / RVOL_slot → ブレイク検知に必要な特徴量を付与して返す。

AM: 09:01–11:30, PM: 12:31–15:29（15:30は大引けオークションとして除外）
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, replace
from typing import Tuple

import numpy as np
import pandas as pd
import xlwings as xw


@dataclass(frozen=True)
class BreakConfig:
    rvol_days: int = 20
    hhv_window: int = 60
    atr_span: int = 14
    eps_pct: float = 0.0005
    atr_buf: float = 0.2
    wick_max: float = 0.35
    rvol_trig: float = 2.0
    rvol_conf: float = 1.2
    vwapslope_look: int = 3
    confirm_look: int = 3
    pullback_buf_atr: float = 0.3


BOOK_NAME_DEFAULT = "rakuten_rss_driver.xlsx"
SHEET_NAME_DEFAULT = "RssChart"
VALID_FEET = {"T","1M","2M","3M","4M","5M","10M","15M","30M","60M","2H","4H","8H","D","W","M"}


class RSSExcelDriver:
    def __init__(self, xlsx_path: str = BOOK_NAME_DEFAULT, *, visible: bool = True,
                 app: xw.App | None = None, config: BreakConfig | dict | None = None) -> None:
        self.xlsx_path = os.path.abspath(xlsx_path)
        self._own_app = app is None
        self.app = app or xw.App(add_book=False)
        self.app.visible = visible
        self.app.display_alerts = False
        self.app.screen_updating = True
        self.wb = self._get_or_create_workbook()
        self.chart_ws = self._get_or_create_sheet(SHEET_NAME_DEFAULT)
        self.cfg = self._coerce_config(config)

    # --------------------------
    # Config helpers
    # --------------------------
    @staticmethod
    def _coerce_config(cfg: BreakConfig | dict | None) -> BreakConfig:
        if cfg is None:
            return BreakConfig()
        if isinstance(cfg, BreakConfig):
            return cfg
        if isinstance(cfg, dict):
            allowed = {k: v for k, v in cfg.items() if k in BreakConfig.__annotations__}
            return replace(BreakConfig(), **allowed)
        raise TypeError("config は BreakConfig か dict を指定してください。")

    def set_config(self, **kwargs) -> None:
        """インスタンス設定を差し替え（指定キーのみ更新）。"""
        allowed = {k: v for k, v in kwargs.items() if k in BreakConfig.__annotations__}
        self.cfg = replace(self.cfg, **allowed)

    # --------------------------
    # Excel helpers
    # --------------------------
    def _get_or_create_workbook(self) -> xw.Book:
        base = os.path.basename(self.xlsx_path).lower()
        for b in self.app.books:
            if b.name.lower() == base:
                return b
        if os.path.exists(self.xlsx_path):
            return self.app.books.open(self.xlsx_path)
        wb = self.app.books.add()
        wb.save(self.xlsx_path)
        return wb

    def _get_or_create_sheet(self, name: str) -> xw.main.Sheet:
        try:
            return self.wb.sheets[name]
        except Exception:
            return self.wb.sheets.add(name, after=self.wb.sheets[-1])

    def _clear_sheet(self) -> None:
        self.chart_ws.clear_contents()

    def _write_rss_chart(self, code: str, foot: str, bars: int) -> None:
        if foot not in VALID_FEET:
            raise ValueError(f"足種 '{foot}' が不正。許可: {sorted(VALID_FEET)}")
        if not (1 <= bars <= 3000):
            raise ValueError("表示本数は 1〜3000 にしてください。")
        self.chart_ws.range("A1").formula = f'=RssChart(,"{code}","{foot}",{bars})'
        try:
            self.app.api.Calculate()
        except Exception:
            pass

    def _read_rss_chart(self, *, timeout: float = 10.0) -> pd.DataFrame:
        """シートD2起点のスピル表（ヘッダーあり）を待ち合わせてDataFrame化。"""
        start = time.time()
        last_rows = 0
        df = None
        while time.time() - start < timeout:
            df = self.chart_ws.range("D2").expand().options(pd.DataFrame, header=1, index=False).value
            if df is None or len(df) == 0:
                time.sleep(0.2);
                continue
            rows = len(df)
            if last_rows != rows:
                last_rows = rows
                time.sleep(0.2);
                continue
            break
        if df is None or df.empty:
            raise RuntimeError("RssChart の結果が空。ログイン/銘柄/足種/ヘッダー行を確認。")
        return df

    def _get_chart(self, code: str, foot: str = "1M", bars: int = 300) -> pd.DataFrame:
        self._clear_sheet()
        self._write_rss_chart(code, foot, bars)
        return self._read_rss_chart()

    # --------------------------
    # 整形 + 特徴量付与（ブレイク判定に必要な列）
    # --------------------------
    def _enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        入力: ['日付','時刻','始値','高値','安値','終値','出来高']
        出力: ブレイク検知に必要な列を含む DataFrame
        """
        cfg = self.cfg
        d = df.copy()

        # 列名・型
        colmap = {'日付':'date','時刻':'time','始値':'open','高値':'high','安値':'low','終値':'close','出来高':'volume'}
        d = d.rename(columns=colmap)
        d['date'] = d['date'].astype(str).str.replace(r'[^0-9/]', '', regex=True)
        d['time'] = d['time'].astype(str).str.replace(r'[^0-9:]', '', regex=True)
        d['timestamp'] = pd.to_datetime(d['date'] + ' ' + d['time'], errors='coerce')
        for c in ['open','high','low','close','volume']:
            d[c] = pd.to_numeric(d[c], errors='coerce')

        # 無効行除外（欠損/出来高0）
        d = d.dropna(subset=['timestamp','open','high','low','close','volume'])
        d = d[d['volume'] > 0].copy()
        d['day'] = d['timestamp'].dt.date

        # セッション/オークション/スロット
        mins = d['timestamp'].dt.hour*60 + d['timestamp'].dt.minute
        d['is_auction'] = mins.isin([9*60, 12*60+30, 15*60+30])

        AM_START, AM_END = 9*60+1, 11*60+30
        PM_START, PM_END = 12*60+31, 15*60+29   # ← 15:29までPM扱い

        am_mask = (mins >= AM_START) & (mins <= AM_END)
        pm_mask = (mins >= PM_START) & (mins <= PM_END)

        d['session'] = np.where(am_mask, 'AM', np.where(pm_mask, 'PM', None))
        d['slot'] = np.where(am_mask, mins - AM_START,
                        np.where(pm_mask, mins - PM_START, np.nan)).astype(float)

        # ▼ index 正規化 & セッション行マスク（以降の transform を安全に適用）
        d = d.sort_values(['day','session','timestamp']).reset_index(drop=True)
        m_sess = d['session'].notna()

        # セッションVWAP（オークションは除外）
        d['tp'] = (d['high'] + d['low'] + d['close']) / 3.0
        d['pv'] = np.where(~d['is_auction'] & d['session'].notna(), d['tp']*d['volume'], 0.0)
        d['vv'] = np.where(~d['is_auction'] & d['session'].notna(), d['volume'], 0.0)
        d[['cum_pv','cum_v']] = d.groupby(['day','session'])[['pv','vv']].cumsum()
        d['vwap_session'] = np.where(d['cum_v']>0, d['cum_pv']/d['cum_v'], np.nan)

        # RVOL_slot（同一(session,slot)×日 の出来高合計 → 過去 cfg.rvol_days 日の中央値/当日除外）
        per_day = (d[~d['is_auction'] & d['slot'].notna()]
                    .groupby(['day','session','slot'], as_index=False)['volume']
                    .sum()
                    .sort_values(['session','slot','day'])
                    .reset_index(drop=True))
        per_day['base'] = (per_day.groupby(['session','slot'])['volume']
                            .transform(lambda s: s.shift(1)
                                                .rolling(cfg.rvol_days, min_periods=5)
                                                .median()))
        d = d.merge(per_day[['day','session','slot','base']],
                    on=['day','session','slot'], how='left', copy=False)
        d['rvol_slot'] = np.where(d['base']>0, d['volume']/d['base'], np.nan)

        # ---------- ブレイク検知用の追加特徴 ----------
        # ATR(14)
        prev_close = d['close'].shift(1)
        tr = np.maximum(d['high']-d['low'],
              np.maximum((d['high']-prev_close).abs(), (d['low']-prev_close).abs()))
        d['atr14'] = tr.ewm(span=cfg.atr_span, adjust=False).mean()

        # 上ヒゲ比率
        rng = (d['high'] - d['low']).replace(0, np.nan)
        d['upper_wick_ratio'] = (d['high'] - d[['open','close']].max(axis=1)) / rng

        # HHV_N（直前まで）: セッション内で算出 → 部分 transform を ndarray で代入
        hhv_minp = max(5, cfg.hhv_window//3)
        _hhv = (d.loc[m_sess]
                  .groupby(['day','session'])['high']
                  .transform(lambda s: s.shift(1).rolling(cfg.hhv_window, min_periods=hhv_minp).max())
                  .to_numpy())
        hhv = pd.Series(np.full(len(d), np.nan), index=d.index, dtype='float64')
        hhv.loc[m_sess] = _hhv
        d['hhvN'] = hhv

        # VWAP勾配（直近 vwapslope_look 本で上向きか）: 同様に位置代入
        _vup = (d.loc[m_sess]
                  .groupby(['day','session'])['vwap_session']
                  .transform(lambda s: s - s.shift(cfg.vwapslope_look))
                  .to_numpy())
        vup = pd.Series(np.full(len(d), np.nan), index=d.index, dtype='float64')
        vup.loc[m_sess] = _vup
        d['vwap_up3'] = vup

        # ブレイク水準とトリガ
        eps_abs = d['close'].abs()*cfg.eps_pct
        level_buf = np.maximum(eps_abs, cfg.atr_buf * d['atr14'])
        d['break_level'] = d['hhvN'] + level_buf

        cond_price = d['close'] > d['break_level']
        cond_vwap  = (d['close'] > d['vwap_session']) & (d['vwap_up3'] > 0)
        cond_rvol  = d['rvol_slot'] >= cfg.rvol_trig
        cond_wick  = (d['upper_wick_ratio'] <= cfg.wick_max)
        d['break_trigger'] = cond_price & cond_vwap & cond_rvol & cond_wick & m_sess & (~d['is_auction'])

        # 確証（次の cfg.confirm_look 本のうち 2条件以上）: VWAP上維持 / RVOL維持 / 押しが浅い
        v_above = (d['close'] > d['vwap_session']).astype(float)
        v_above.loc[~m_sess] = np.nan

        _nv = (v_above.loc[m_sess]
                 .groupby([d.loc[m_sess,'day'], d.loc[m_sess,'session']])
                 .transform(lambda s: s.shift(-1).rolling(cfg.confirm_look).sum())
                 .to_numpy())
        d['next_vwap_count'] = np.nan
        d.loc[m_sess, 'next_vwap_count'] = _nv

        _nr = (d.loc[m_sess, 'rvol_slot']
                 .groupby([d.loc[m_sess,'day'], d.loc[m_sess,'session']])
                 .transform(lambda s: s.shift(-1).rolling(cfg.confirm_look).mean())
                 .to_numpy())
        d['next_rvol_mean'] = np.nan
        d.loc[m_sess, 'next_rvol_mean'] = _nr

        _nl = (d.loc[m_sess, 'low']
                 .groupby([d.loc[m_sess,'day'], d.loc[m_sess,'session']])
                 .transform(lambda s: s.shift(-1).rolling(cfg.confirm_look).min())
                 .to_numpy())
        d['next_low_min'] = np.nan
        d.loc[m_sess, 'next_low_min'] = _nl

        keep_ok1 = (d['next_vwap_count'] >= 2).fillna(False)
        keep_ok2 = (d['next_rvol_mean'] >= cfg.rvol_conf).fillna(False)
        pb_floor = d['break_level'] - cfg.pullback_buf_atr * d['atr14']
        keep_ok3 = (d['next_low_min'] >= pb_floor).fillna(False)
        d['break_confirmed'] = d['break_trigger'] & ((keep_ok1 + keep_ok2 + keep_ok3) >= 2)

        # 出力列（見やすい順）
        keep_cols = [
            'timestamp','date','time','session','slot','is_auction',
            'open','high','low','close','volume',
            'vwap_session','rvol_slot',
            'atr14','hhvN','upper_wick_ratio','vwap_up3',
            'break_level','break_trigger','break_confirmed'
        ]
        return d[keep_cols].sort_values('timestamp').reset_index(drop=True)

    # 公開API
    def get_chart(self, code: str, foot: str = "1M", bars: int = 2000) -> pd.DataFrame:
        """RssChart→整形→VWAP/RVOL/ATR/HHV/ヒゲ等→ブレイク判定列まで付与。"""
        raw = self._get_chart(code, foot, bars)
        return self._enrich(raw)

    def close(self, *, save: bool = True, close_book: bool = False) -> None:
        try:
            if save:
                self.wb.save(self.xlsx_path)
        except Exception:
            pass
        try:
            if close_book:
                self.wb.close()
        except Exception:
            pass
        if self._own_app:
            try:
                self.app.quit()
            except Exception:
                pass


def find_driver_book(book_name: str = BOOK_NAME_DEFAULT, *, timeout: float = 10.0, poll: float = 0.25) -> Tuple[xw.App, xw.Book]:
    end = time.time() + timeout
    target = book_name.lower()
    while time.time() < end:
        for app in list(xw.apps):
            try:
                for b in app.books:
                    if b.name.lower() == target:
                        return app, b
            except Exception:
                pass
        time.sleep(poll)
    raise TimeoutError(f"既存の Excel から '{book_name}' が見つかりません。手動で開いてから再実行してください。")


if __name__ == "__main__":
    try:
        app, wb = find_driver_book(timeout=30)
        drv = RSSExcelDriver(visible=True, app=app, config=BreakConfig())
    except Exception:
        drv = RSSExcelDriver(visible=True)

    try:
        df = drv.get_chart("7203.T", "1M", 1000)
        print(df.tail(50))
    finally:
        drv.close(save=True, close_book=False)
