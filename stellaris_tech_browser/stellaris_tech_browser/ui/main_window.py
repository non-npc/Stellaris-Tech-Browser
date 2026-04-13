from __future__ import annotations

import configparser
import sqlite3
import sys
from pathlib import Path
from typing import List, Optional

from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QAction
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)

from ..exporters import export_json_from_db
from ..scanner import StellarisTechScanner


class ScanWorker(QThread):
    progress_changed = pyqtSignal(int, str, str)
    finished_ok = pyqtSignal(str)
    failed = pyqtSignal(str)

    def __init__(self, game_path: str, mods_path: str, db_path: str, language: str) -> None:
        super().__init__()
        self.game_path = game_path
        self.mods_path = mods_path
        self.db_path = db_path
        self.language = language

    def run(self) -> None:
        try:
            scanner = StellarisTechScanner(
                game_path=Path(self.game_path),
                mods_path=Path(self.mods_path) if self.mods_path else None,
                db_path=Path(self.db_path),
                language=self.language,
            )
            result = scanner.scan(lambda p, phase, detail: self.progress_changed.emit(p, phase, detail))
            self.finished_ok.emit(str(result))
        except Exception as exc:
            self.failed.emit(str(exc))


class MainWindow(QMainWindow):
    def __init__(self) -> None:
        super().__init__()
        self.settings_path = self._get_settings_path()
        self.settings = configparser.ConfigParser()
        self._load_settings()

        self.setWindowTitle('Stellaris Tech Browser')
        self.resize(1400, 900)
        self.current_db_path: Optional[Path] = None
        self.conn: Optional[sqlite3.Connection] = None
        self.worker: Optional[ScanWorker] = None

        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        self.scan_tab = QWidget()
        self.browser_tab = QWidget()
        self.tabs.addTab(self.scan_tab, 'Scan')
        self.tabs.addTab(self.browser_tab, 'Browse')

        self._build_scan_tab()
        self._build_browser_tab()
        self._build_menu()
        self._apply_saved_settings()

    def _get_settings_path(self) -> Path:
        app_dir = Path(sys.argv[0]).resolve().parent
        return app_dir / 'stellaris_tech_browser.ini'

    def _load_settings(self) -> None:
        if self.settings_path.exists():
            self.settings.read(self.settings_path, encoding='utf-8')
        if 'paths' not in self.settings:
            self.settings['paths'] = {}

    def _save_settings(self) -> None:
        self.settings['paths']['game_folder'] = self.game_path_edit.text().strip()
        self.settings['paths']['mods_folder'] = self.mods_path_edit.text().strip()
        self.settings['paths']['database_output'] = self.db_path_edit.text().strip()
        self.settings['paths']['language'] = self.language_combo.currentText()
        self.settings_path.parent.mkdir(parents=True, exist_ok=True)
        with self.settings_path.open('w', encoding='utf-8') as handle:
            self.settings.write(handle)

    def _apply_saved_settings(self) -> None:
        paths = self.settings['paths']
        self.game_path_edit.setText(paths.get('game_folder', ''))
        self.mods_path_edit.setText(paths.get('mods_folder', ''))
        default_db = str(Path.cwd() / 'stellaris_tech.db')
        self.db_path_edit.setText(paths.get('database_output', default_db))
        language = paths.get('language', 'english')
        idx = self.language_combo.findText(language)
        if idx >= 0:
            self.language_combo.setCurrentIndex(idx)

    def _build_menu(self) -> None:
        menu = self.menuBar().addMenu('&File')
        open_db_action = QAction('Open Database...', self)
        open_db_action.triggered.connect(self.choose_database)
        menu.addAction(open_db_action)

        export_action = QAction('Export JSON...', self)
        export_action.triggered.connect(self.export_json)
        menu.addAction(export_action)

    def _build_scan_tab(self) -> None:
        layout = QVBoxLayout(self.scan_tab)

        form_box = QGroupBox('Scan Configuration')
        form = QFormLayout(form_box)

        self.game_path_edit = QLineEdit()
        self.mods_path_edit = QLineEdit()
        self.db_path_edit = QLineEdit(str(Path.cwd() / 'stellaris_tech.db'))
        self.language_combo = QComboBox()
        self.language_combo.addItems(['english', 'french', 'german', 'spanish', 'russian'])
        self.language_combo.currentTextChanged.connect(lambda _text: self._save_settings())
        self.game_path_edit.editingFinished.connect(self._save_settings)
        self.mods_path_edit.editingFinished.connect(self._save_settings)
        self.db_path_edit.editingFinished.connect(self._save_settings)

        browse_game = QPushButton('Browse...')
        browse_game.clicked.connect(lambda: self._pick_folder(self.game_path_edit))
        browse_mods = QPushButton('Browse...')
        browse_mods.clicked.connect(lambda: self._pick_folder(self.mods_path_edit))
        browse_db = QPushButton('Browse...')
        browse_db.clicked.connect(self._pick_db_path)

        row1 = QHBoxLayout(); row1.addWidget(self.game_path_edit); row1.addWidget(browse_game)
        row2 = QHBoxLayout(); row2.addWidget(self.mods_path_edit); row2.addWidget(browse_mods)
        row3 = QHBoxLayout(); row3.addWidget(self.db_path_edit); row3.addWidget(browse_db)

        wrapper1 = QWidget(); wrapper1.setLayout(row1)
        wrapper2 = QWidget(); wrapper2.setLayout(row2)
        wrapper3 = QWidget(); wrapper3.setLayout(row3)

        form.addRow('Game Folder', wrapper1)
        form.addRow('Mods Folder', wrapper2)
        form.addRow('Database Output', wrapper3)
        form.addRow('Language', self.language_combo)

        self.scan_button = QPushButton('Scan and Build Database')
        self.scan_button.clicked.connect(self.start_scan)
        self.progress_bar = QProgressBar()
        self.progress_phase_label = QLabel('Idle')
        self.progress_detail_label = QLabel('')
        self.scan_log = QPlainTextEdit()
        self.scan_log.setReadOnly(True)

        layout.addWidget(form_box)
        layout.addWidget(self.scan_button)
        layout.addWidget(self.progress_bar)
        layout.addWidget(self.progress_phase_label)
        layout.addWidget(self.progress_detail_label)
        layout.addWidget(QLabel('Scan Log'))
        layout.addWidget(self.scan_log, 1)

    def _build_browser_tab(self) -> None:
        outer = QVBoxLayout(self.browser_tab)

        filters = QGroupBox('Filters')
        grid = QGridLayout(filters)
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText('Search name, description, or tech key')
        self.area_combo = QComboBox(); self.area_combo.addItem('All Areas')
        self.tier_combo = QComboBox(); self.tier_combo.addItem('All Tiers')
        self.source_combo = QComboBox(); self.source_combo.addItem('All Sources')
        self.repeatable_only = QCheckBox('Repeatable only')
        self.rare_only = QCheckBox('Rare only')
        self.refresh_button = QPushButton('Refresh Results')
        self.refresh_button.clicked.connect(self.refresh_table)

        grid.addWidget(QLabel('Search'), 0, 0)
        grid.addWidget(self.search_edit, 0, 1, 1, 3)
        grid.addWidget(QLabel('Area'), 1, 0)
        grid.addWidget(self.area_combo, 1, 1)
        grid.addWidget(QLabel('Tier'), 1, 2)
        grid.addWidget(self.tier_combo, 1, 3)
        grid.addWidget(QLabel('Source'), 2, 0)
        grid.addWidget(self.source_combo, 2, 1)
        grid.addWidget(self.repeatable_only, 2, 2)
        grid.addWidget(self.rare_only, 2, 3)
        grid.addWidget(self.refresh_button, 3, 3)

        splitter = QSplitter()
        self.table = QTableWidget(0, 8)
        self.table.setHorizontalHeaderLabels(['Name', 'Key', 'Area', 'Tier', 'Categories', 'Source', 'Prereqs', 'Unlocks'])
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.itemSelectionChanged.connect(self.load_selected_tech)
        self.table.setSortingEnabled(True)

        right = QWidget()
        right_layout = QVBoxLayout(right)
        self.detail_header = QLabel('No technology selected')
        self.detail_body = QTextEdit(); self.detail_body.setReadOnly(True)
        self.raw_body = QPlainTextEdit(); self.raw_body.setReadOnly(True)
        self.warnings_body = QPlainTextEdit(); self.warnings_body.setReadOnly(True)
        right_tabs = QTabWidget()
        right_tabs.addTab(self.detail_body, 'Details')
        right_tabs.addTab(self.raw_body, 'Raw')
        right_tabs.addTab(self.warnings_body, 'Warnings')
        right_layout.addWidget(self.detail_header)
        right_layout.addWidget(right_tabs, 1)

        splitter.addWidget(self.table)
        splitter.addWidget(right)
        splitter.setSizes([850, 550])

        outer.addWidget(filters)
        outer.addWidget(splitter, 1)

    def _pick_folder(self, edit: QLineEdit) -> None:
        path = QFileDialog.getExistingDirectory(self, 'Select Folder', edit.text() or str(Path.home()))
        if path:
            edit.setText(path)
            self._save_settings()

    def _pick_db_path(self) -> None:
        path, _ = QFileDialog.getSaveFileName(self, 'Select Database File', self.db_path_edit.text(), 'SQLite DB (*.db *.sqlite)')
        if path:
            self.db_path_edit.setText(path)
            self._save_settings()

    def choose_database(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, 'Open Database', str(Path.home()), 'SQLite DB (*.db *.sqlite)')
        if not path:
            return
        self.open_database(Path(path))

    def start_scan(self) -> None:
        game_path = self.game_path_edit.text().strip()
        db_path = self.db_path_edit.text().strip()
        if not game_path or not Path(game_path).exists():
            QMessageBox.warning(self, 'Missing Game Folder', 'Please select a valid Stellaris game folder.')
            return
        if not db_path:
            QMessageBox.warning(self, 'Missing Database Path', 'Please select an output database path.')
            return
        self._save_settings()
        self.scan_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.scan_log.clear()
        self.worker = ScanWorker(game_path, self.mods_path_edit.text().strip(), db_path, self.language_combo.currentText())
        self.worker.progress_changed.connect(self.on_progress)
        self.worker.finished_ok.connect(self.on_scan_complete)
        self.worker.failed.connect(self.on_scan_failed)
        self.worker.start()

    def on_progress(self, percent: int, phase: str, detail: str) -> None:
        self.progress_bar.setValue(percent)
        self.progress_phase_label.setText(phase)
        self.progress_detail_label.setText(detail)
        self.scan_log.appendPlainText(f'[{percent:03d}%] {phase} :: {detail}')

    def on_scan_complete(self, db_path: str) -> None:
        self.scan_button.setEnabled(True)
        self.current_db_path = Path(db_path)
        self.scan_log.appendPlainText(f'Completed: {db_path}')
        self.open_database(Path(db_path))
        self.tabs.setCurrentWidget(self.browser_tab)

    def on_scan_failed(self, message: str) -> None:
        self.scan_button.setEnabled(True)
        self.scan_log.appendPlainText(f'ERROR: {message}')
        QMessageBox.critical(self, 'Scan Failed', message)

    def open_database(self, path: Path) -> None:
        if self.conn is not None:
            self.conn.close()
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.current_db_path = path
        self.populate_filter_values()
        self.refresh_table()

    def populate_filter_values(self) -> None:
        if not self.conn:
            return
        self.area_combo.blockSignals(True)
        self.tier_combo.blockSignals(True)
        self.source_combo.blockSignals(True)
        self.area_combo.clear(); self.area_combo.addItem('All Areas')
        self.tier_combo.clear(); self.tier_combo.addItem('All Tiers')
        self.source_combo.clear(); self.source_combo.addItem('All Sources')
        for row in self.conn.execute('SELECT DISTINCT area FROM technologies WHERE area IS NOT NULL AND area <> "" ORDER BY area'):
            self.area_combo.addItem(row['area'])
        for row in self.conn.execute('SELECT DISTINCT tier FROM technologies WHERE tier IS NOT NULL ORDER BY tier'):
            self.tier_combo.addItem(str(row['tier']))
        for row in self.conn.execute('SELECT DISTINCT COALESCE(mod_name, source_type) AS label FROM technologies t LEFT JOIN sources s ON t.source_id=s.id ORDER BY label'):
            self.source_combo.addItem(row['label'])
        self.area_combo.blockSignals(False)
        self.tier_combo.blockSignals(False)
        self.source_combo.blockSignals(False)

    def refresh_table(self) -> None:
        if not self.conn:
            return
        where = []
        params: List[object] = []
        text = self.search_edit.text().strip()
        if text:
            where.append('t.id IN (SELECT rowid FROM technology_search WHERE technology_search MATCH ?)')
            params.append(text.replace('"', ' '))
        if self.area_combo.currentText() != 'All Areas':
            where.append('t.area = ?')
            params.append(self.area_combo.currentText())
        if self.tier_combo.currentText() != 'All Tiers':
            where.append('t.tier = ?')
            params.append(int(self.tier_combo.currentText()))
        if self.source_combo.currentText() != 'All Sources':
            where.append('COALESCE(s.mod_name, s.source_type) = ?')
            params.append(self.source_combo.currentText())
        if self.repeatable_only.isChecked():
            where.append('t.is_repeatable = 1')
        if self.rare_only.isChecked():
            where.append('t.is_rare = 1')
        sql = '''
            SELECT t.id, t.tech_key, t.display_name, t.area, t.tier,
                   COALESCE(s.mod_name, s.source_type) AS source_label,
                   (SELECT COUNT(*) FROM technology_prerequisites tp WHERE tp.tech_id = t.id) AS prereq_count,
                   (SELECT COUNT(*) FROM technology_prerequisites tp WHERE tp.prerequisite_tech_id = t.id) AS unlock_count,
                   GROUP_CONCAT(tc.category, ', ') AS categories
            FROM technologies t
            LEFT JOIN sources s ON t.source_id = s.id
            LEFT JOIN technology_categories tc ON tc.tech_id = t.id
        '''
        if where:
            sql += ' WHERE ' + ' AND '.join(where)
        sql += ' GROUP BY t.id ORDER BY t.area, t.tier, t.display_name'
        rows = self.conn.execute(sql, params).fetchall()
        self.table.setRowCount(len(rows))
        for r, row in enumerate(rows):
            values = [
                row['display_name'] or row['tech_key'],
                row['tech_key'],
                row['area'] or '',
                '' if row['tier'] is None else str(row['tier']),
                row['categories'] or '',
                row['source_label'] or '',
                str(row['prereq_count']),
                str(row['unlock_count']),
            ]
            for c, value in enumerate(values):
                item = QTableWidgetItem(value)
                if c == 0:
                    item.setData(256, row['id'])
                self.table.setItem(r, c, item)
        if rows:
            self.table.selectRow(0)

    def load_selected_tech(self) -> None:
        if not self.conn:
            return
        items = self.table.selectedItems()
        if not items:
            return
        row = items[0].row()
        tech_id = self.table.item(row, 0).data(256)
        tech = self.conn.execute(
            '''
            SELECT t.*, COALESCE(s.mod_name, s.source_type) AS source_label, s.root_path, f.relative_path
            FROM technologies t
            LEFT JOIN sources s ON t.source_id = s.id
            LEFT JOIN files f ON t.file_id = f.id
            WHERE t.id = ?
            ''',
            (tech_id,),
        ).fetchone()
        if not tech:
            return

        prereqs = [r['tech_key'] for r in self.conn.execute(
            'SELECT t2.tech_key FROM technology_prerequisites tp JOIN technologies t2 ON tp.prerequisite_tech_id=t2.id WHERE tp.tech_id=? ORDER BY t2.tech_key',
            (tech_id,),
        )]
        unlocks = [r['tech_key'] for r in self.conn.execute(
            'SELECT t2.tech_key FROM technology_prerequisites tp JOIN technologies t2 ON tp.tech_id=t2.id WHERE tp.prerequisite_tech_id=? ORDER BY t2.tech_key',
            (tech_id,),
        )]
        unlockables = [f"{r['unlock_type']}: {r['unlock_key']}" for r in self.conn.execute(
            'SELECT u.unlock_type, u.unlock_key FROM technology_unlocks tu JOIN unlockables u ON tu.unlockable_id=u.id WHERE tu.tech_id=? ORDER BY u.unlock_type, u.unlock_key',
            (tech_id,),
        )]
        categories = [r['category'] for r in self.conn.execute('SELECT category FROM technology_categories WHERE tech_id=? ORDER BY category', (tech_id,))]

        self.detail_header.setText(tech['display_name'] or tech['tech_key'])
        detail = []
        detail.append(f"<b>Key:</b> {tech['tech_key']}")
        detail.append(f"<b>Area:</b> {tech['area'] or ''}")
        detail.append(f"<b>Tier:</b> {tech['tier'] if tech['tier'] is not None else ''}")
        detail.append(f"<b>Categories:</b> {', '.join(categories)}")
        detail.append(f"<b>Source:</b> {tech['source_label'] or ''}")
        detail.append(f"<b>Defined In:</b> {tech['relative_path'] or ''}")
        detail.append(f"<b>Game/Mod Root:</b> {tech['root_path'] or ''}")
        detail.append(f"<b>Repeatable:</b> {'Yes' if tech['is_repeatable'] else 'No'}")
        detail.append(f"<b>Rare:</b> {'Yes' if tech['is_rare'] else 'No'}")
        detail.append(f"<b>Dangerous:</b> {'Yes' if tech['is_dangerous'] else 'No'}")
        detail.append(f"<b>Gateway:</b> {tech['gateway'] or ''}")
        detail.append(f"<b>AI Update Type:</b> {tech['ai_update_type'] or ''}")
        detail.append(f"<b>Description:</b><br>{(tech['description'] or '').replace(chr(10), '<br>')}")
        detail.append(f"<b>Prerequisites:</b> {', '.join(prereqs) if prereqs else 'None'}")
        detail.append(f"<b>Unlocks Techs:</b> {', '.join(unlocks) if unlocks else 'None'}")
        detail.append(f"<b>Unlocks Content:</b><br>{'<br>'.join(unlockables) if unlockables else 'None'}")
        self.detail_body.setHtml('<br><br>'.join(detail))
        self.raw_body.setPlainText(tech['raw_block_json'] or '')

        warnings = [f"[{r['severity']}] {r['warning_type']}: {r['message']}" for r in self.conn.execute('SELECT severity, warning_type, message FROM warnings ORDER BY id DESC LIMIT 250')]
        self.warnings_body.setPlainText('\n'.join(warnings))

    def export_json(self) -> None:
        if not self.current_db_path or not self.current_db_path.exists():
            QMessageBox.warning(self, 'No Database', 'Open or build a database first.')
            return
        path, _ = QFileDialog.getSaveFileName(self, 'Export JSON', str(self.current_db_path.with_suffix('.json')), 'JSON Files (*.json)')
        if not path:
            return
        out_path = export_json_from_db(self.current_db_path, Path(path))
        QMessageBox.information(self, 'Export Complete', f'Exported JSON to:\n{out_path}')
