/** 可搜索下拉：保留原生 select 作为值源，叠加搜索面板。 */
(function initSearchableSelectModule(global) {
    const REGISTRY = new WeakMap();
    let documentListenersBound = false;

    function normalize(text) {
        return String(text || '').trim().toLocaleLowerCase();
    }

    function optionMatches(option, query) {
        if (!query) return true;
        const label = option.textContent || option.label || option.value;
        return normalize(label).includes(normalize(query));
    }

    function getState(select) {
        return select ? REGISTRY.get(select) || null : null;
    }

    function updateTrigger(state) {
        const selected = state.select.selectedOptions[0];
        state.triggerLabel.textContent = selected
            ? selected.textContent
            : (state.config.placeholder || '请选择');
        state.trigger.disabled = !!state.select.disabled;
        state.wrapper.classList.toggle('is-disabled', !!state.select.disabled);
    }

    function ensureMeasurer(state) {
        if (!state.measurer) {
            state.measurer = document.createElement('span');
            state.measurer.className = 'searchable-select__measure';
            state.measurer.setAttribute('aria-hidden', 'true');
            document.body.appendChild(state.measurer);
        }
        return state.measurer;
    }

    function measureOptionTextWidth(state, text) {
        const measurer = ensureMeasurer(state);
        const triggerStyle = getComputedStyle(state.trigger);
        measurer.style.font = triggerStyle.font;
        measurer.style.letterSpacing = triggerStyle.letterSpacing;
        measurer.textContent = text;
        return measurer.offsetWidth;
    }

    function shouldApplyAutoWidth(state) {
        if (!state?.config?.autoWidthFromOptions) return false;
        if (window.matchMedia('(max-width: 768px)').matches) return false;
        return true;
    }

    function clearAutoWidth(state) {
        state.wrapper.style.width = '';
        state.trigger.style.width = '';
        state.wrapper.classList.remove('searchable-select--auto-width');
    }

    function applyAutoWidthFromOptions(state) {
        if (!state?.config?.autoWidthFromOptions) return;
        if (!shouldApplyAutoWidth(state)) {
            clearAutoWidth(state);
            return;
        }
        let maxText = 0;
        [...state.select.options].forEach((option) => {
            if (option.hidden) return;
            const text = String(option.textContent || '').trim();
            if (!text) return;
            maxText = Math.max(maxText, measureOptionTextWidth(state, text));
        });
        const triggerStyle = getComputedStyle(state.trigger);
        const padX = parseFloat(triggerStyle.paddingLeft) + parseFloat(triggerStyle.paddingRight);
        const borderX = parseFloat(triggerStyle.borderLeftWidth) + parseFloat(triggerStyle.borderRightWidth);
        const arrowReserve = 24;
        const minWidth = Number.isFinite(state.config.autoWidthMin)
            ? state.config.autoWidthMin
            : 0;
        const maxWidth = Number.isFinite(state.config.autoWidthMax)
            ? state.config.autoWidthMax
            : Number.POSITIVE_INFINITY;
        const width = Math.ceil(Math.min(
            maxWidth,
            Math.max(minWidth, maxText + padX + borderX + arrowReserve),
        ));
        state.wrapper.style.width = `${width}px`;
        state.trigger.style.width = `${width}px`;
        state.wrapper.classList.add('searchable-select--auto-width');
    }

    let autoWidthResizeBound = false;

    function bindAutoWidthResize() {
        if (autoWidthResizeBound) return;
        autoWidthResizeBound = true;
        window.addEventListener('resize', () => {
            document.querySelectorAll('select.searchable-select__native').forEach((select) => {
                const state = getState(select);
                if (state?.config?.autoWidthFromOptions) {
                    applyAutoWidthFromOptions(state);
                    if (state.open) positionPanel(state);
                }
            });
        });
    }

    function buildList(state) {
        const { select, list, searchInput, empty, config } = state;
        const query = String(searchInput.value || '').trim();
        const pinned = new Set(config.pinnedValues || []);
        list.innerHTML = '';
        let visible = 0;
        [...select.options].forEach((option) => {
            if (option.hidden) return;
            const isPinned = pinned.has(option.value);
            if (!isPinned && !optionMatches(option, query)) return;
            const item = document.createElement('li');
            item.className = 'searchable-select__option';
            item.setAttribute('role', 'option');
            item.dataset.value = option.value;
            item.textContent = option.textContent;
            if (option.value === select.value) {
                item.classList.add('is-selected');
                item.setAttribute('aria-selected', 'true');
            }
            item.addEventListener('mousedown', (event) => event.preventDefault());
            item.addEventListener('click', () => chooseOption(state, option.value));
            list.appendChild(item);
            visible += 1;
        });
        empty.hidden = visible > 0;
        state.activeIndex = -1;
    }

    function chooseOption(state, value) {
        const { select } = state;
        const changed = select.value !== value;
        select.value = value;
        updateTrigger(state);
        close(state);
        if (changed) {
            select.dispatchEvent(new Event('change', { bubbles: true }));
        }
    }

    function unbindPanelPositionListeners(state) {
        if (!state._positionHandler) return;
        window.removeEventListener('resize', state._positionHandler);
        window.removeEventListener('scroll', state._positionHandler, true);
        state._positionHandler = null;
    }

    function resetPanelPosition(state) {
        const { panel, wrapper } = state;
        panel.classList.remove('is-floating');
        panel.style.position = '';
        panel.style.left = '';
        panel.style.top = '';
        panel.style.width = '';
        panel.style.maxHeight = '';
        panel.style.zIndex = '';
        if (panel.parentNode !== wrapper) {
            wrapper.appendChild(panel);
        }
        state.panelPortaled = false;
        unbindPanelPositionListeners(state);
    }

    function positionPanel(state) {
        const { panel, trigger, config } = state;
        const rect = trigger.getBoundingClientRect();
        const minWidth = Number.isFinite(config.panelMinWidth) ? config.panelMinWidth : 0;
        const width = minWidth > 0 ? Math.max(rect.width, minWidth) : rect.width;
        const maxHeight = Math.min(320, Math.max(160, window.innerHeight - 16));

        if (panel.parentNode !== document.body) {
            document.body.appendChild(panel);
        }
        state.panelPortaled = true;
        panel.classList.add('is-floating');
        panel.hidden = false;
        panel.style.position = 'fixed';
        panel.style.width = `${width}px`;
        panel.style.maxHeight = `${maxHeight}px`;
        panel.style.zIndex = '10050';

        let top = rect.bottom + 4;
        const panelHeight = panel.offsetHeight || maxHeight;
        if (top + panelHeight > window.innerHeight - 8) {
            top = Math.max(8, rect.top - panelHeight - 4);
        }
        let left = rect.left;
        if (left + width > window.innerWidth - 8) {
            left = Math.max(8, window.innerWidth - width - 8);
        }
        panel.style.left = `${left}px`;
        panel.style.top = `${top}px`;
    }

    function bindPanelPositionListeners(state) {
        unbindPanelPositionListeners(state);
        const handler = () => {
            if (state.open) positionPanel(state);
        };
        state._positionHandler = handler;
        window.addEventListener('resize', handler);
        window.addEventListener('scroll', handler, true);
    }

    function close(state) {
        if (!state || !state.open) return;
        state.open = false;
        state.panel.hidden = true;
        state.trigger.setAttribute('aria-expanded', 'false');
        state.wrapper.classList.remove('is-open');
        state.searchInput.value = '';
        resetPanelPosition(state);
    }

    function closeAll(exceptSelect) {
        document.querySelectorAll('.searchable-select.is-open').forEach((wrapper) => {
            const select = wrapper.querySelector('select.searchable-select__native');
            if (!select || select === exceptSelect) return;
            const state = getState(select);
            if (state) close(state);
        });
    }

    function open(state) {
        if (!state || state.select.disabled) return;
        closeAll(state.select);
        state.open = true;
        state.panel.hidden = false;
        state.trigger.setAttribute('aria-expanded', 'true');
        state.wrapper.classList.add('is-open');
        state.searchInput.value = '';
        buildList(state);
        const optionCount = [...state.select.options].filter((opt) => !opt.hidden).length;
        const minForSearch = Number.isFinite(state.config.minOptionsForSearch)
            ? state.config.minOptionsForSearch
            : 6;
        const showSearch = optionCount >= minForSearch;
        state.searchWrap.hidden = !showSearch;
        positionPanel(state);
        bindPanelPositionListeners(state);
        if (showSearch) {
            window.setTimeout(() => state.searchInput.focus(), 0);
        }
    }

    function moveActive(state, delta) {
        const items = [...state.list.querySelectorAll('.searchable-select__option:not([hidden])')];
        if (!items.length) return;
        state.activeIndex = Math.max(0, Math.min(items.length - 1, state.activeIndex + delta));
        items.forEach((item, index) => {
            item.classList.toggle('is-active', index === state.activeIndex);
        });
        const active = items[state.activeIndex];
        if (active) active.scrollIntoView({ block: 'nearest' });
    }

    function bindDocumentListeners() {
        if (documentListenersBound) return;
        documentListenersBound = true;
        document.addEventListener('click', (event) => {
            const target = event.target;
            if (!(target instanceof Element)) return;
            if (target.closest('.searchable-select')
                || target.closest('.searchable-select__panel')) return;
            closeAll();
        });
        document.addEventListener('keydown', (event) => {
            if (event.key !== 'Escape') return;
            closeAll();
        });
    }

    function createState(select, config) {
        const wrapper = document.createElement('div');
        wrapper.className = 'searchable-select';
        select.parentNode.insertBefore(wrapper, select);
        wrapper.appendChild(select);

        select.classList.add('searchable-select__native');
        select.tabIndex = -1;
        select.setAttribute('aria-hidden', 'true');

        const trigger = document.createElement('button');
        trigger.type = 'button';
        trigger.className = 'searchable-select__trigger';
        trigger.setAttribute('aria-haspopup', 'listbox');
        trigger.setAttribute('aria-expanded', 'false');
        const triggerLabel = document.createElement('span');
        triggerLabel.className = 'searchable-select__trigger-label';
        trigger.appendChild(triggerLabel);
        wrapper.appendChild(trigger);

        const panel = document.createElement('div');
        panel.className = 'searchable-select__panel';
        panel.hidden = true;

        const searchWrap = document.createElement('div');
        searchWrap.className = 'searchable-select__search-wrap';
        const searchInput = document.createElement('input');
        searchInput.type = 'search';
        searchInput.className = 'searchable-select__search';
        searchInput.placeholder = config.searchPlaceholder || '搜索…';
        searchInput.autocomplete = 'off';
        searchInput.setAttribute('aria-label', config.searchAriaLabel || '搜索选项');
        searchInput.setAttribute('enterkeyhint', 'search');
        searchWrap.appendChild(searchInput);
        panel.appendChild(searchWrap);

        const list = document.createElement('ul');
        list.className = 'searchable-select__list';
        list.setAttribute('role', 'listbox');
        panel.appendChild(list);

        const empty = document.createElement('div');
        empty.className = 'searchable-select__empty';
        empty.hidden = true;
        empty.textContent = config.emptyText || '无匹配项';
        panel.appendChild(empty);

        wrapper.appendChild(panel);

        const state = {
            select,
            wrapper,
            trigger,
            triggerLabel,
            panel,
            searchWrap,
            searchInput,
            list,
            empty,
            config,
            open: false,
            activeIndex: -1,
        };

        trigger.addEventListener('click', () => {
            if (state.open) close(state);
            else open(state);
        });

        searchInput.addEventListener('input', () => {
            buildList(state);
            if (state.open) positionPanel(state);
        });
        searchInput.addEventListener('keydown', (event) => {
            if (event.key === 'ArrowDown') {
                event.preventDefault();
                moveActive(state, 1);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                moveActive(state, -1);
            } else if (event.key === 'Enter') {
                event.preventDefault();
                const active = state.list.querySelector('.searchable-select__option.is-active')
                    || state.list.querySelector('.searchable-select__option');
                if (active) chooseOption(state, active.dataset.value || '');
            } else if (event.key === 'Escape') {
                event.preventDefault();
                close(state);
                state.trigger.focus();
            }
        });

        REGISTRY.set(select, state);
        updateTrigger(state);
        if (config.autoWidthFromOptions) bindAutoWidthResize();
        applyAutoWidthFromOptions(state);
        return state;
    }

    function init(select, config = {}) {
        if (!select || select.tagName !== 'SELECT') return null;
        bindDocumentListeners();
        if (REGISTRY.has(select)) {
            const existing = REGISTRY.get(select);
            existing.config = { ...existing.config, ...config };
            applyAutoWidthFromOptions(existing);
            return existing;
        }
        return createState(select, config);
    }

    function sync(select) {
        const state = getState(select);
        if (!state) return;
        updateTrigger(state);
        applyAutoWidthFromOptions(state);
        if (state.open) {
            buildList(state);
            positionPanel(state);
        }
    }

    function setValue(select, value, options = {}) {
        if (!select) return;
        const silent = !!options.silent;
        const changed = select.value !== value;
        select.value = value;
        sync(select);
        if (changed && !silent) {
            select.dispatchEvent(new Event('change', { bubbles: true }));
        }
    }

    global.SearchableSelect = {
        init,
        sync,
        setValue,
        closeAll,
    };
}(window));
