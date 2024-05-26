const taskTypes = {
  'riceBuilder': {
    'taskName': 'DailyRiceBuilder',
    'taskKey': 'daily-rice-builder-3d'
  },
  'cropBuilder': {
    'taskName': 'DailyCropBuilder',
    'taskKey': 'daily-crop-builder-3d'
  },
  'fruitBuilder': {
    'taskName': 'DailyFruitBuilder',
    'taskKey': 'daily-fruit-builder-3d'
  },
  'flowerBuilder': {
    'taskName': 'DailyFlowerBuilder',
    'taskKey': 'daily-flower-builder-3d'
  },
  'hogBuilder': {
    'taskName': 'DailyHogBuilder',
    'taskKey': 'daily-hog-builder-3d'
  },
  'ramBuilder': {
    'taskName': 'DailyRamBuilder',
    'taskKey': 'daily-ram-builder-3d'
  },
  'chickenBuilder': {
    'taskName': 'DailyChickenBuilder',
    'taskKey': 'daily-chicken-builder-3d'
  },
  'duckBuilder': {
    'taskName': 'DailyDuckBuilder',
    'taskKey': 'daily-duck-builder-3d'
  },
  'gooseBuilder': {
    'taskName': 'DailyGooseBuilder',
    'taskKey': 'daily-goose-builder-3d'
  },
  'cattleBuilder': {
    'taskName': 'DailyCattleBuilder',
    'taskKey': 'daily-cattle-builder-3d'
  },
  'originSeafoodBuilder': {
    'taskName': 'DailyOriginSeafoodBuilder',
    'taskKey': 'daily-seafood-origin-builder-3d'
  },
  'wholesaleSeafoodBuilder': {
    'taskName': 'DailyWholesaleSeafoodBuilder',
    'taskKey': 'daily-seafood-wholesale-builder-3d'
  },
  'feedBuilder': {
    'taskName': 'DailyFeedBuilder',
    'taskKey': 'daily-feed-builder-31d'
  },
  'naifChickensBuilder': {
    'taskName': 'DailyNaifchickensBuilder',
    'taskKey': 'daily-naifchickens-builder-31d'
  }
}

function getTaskObj() {
  let breadcrumb = [...$('.breadcrumb').children()]

  if (breadcrumb[0] && breadcrumb[0].textContent.includes('農產品')) {
    if (breadcrumb[1] && breadcrumb[1].textContent.includes('糧價')) {
      return taskTypes.riceBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('蔬菜')) {
      return taskTypes.cropBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('水果')) {
      return taskTypes.fruitBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('花卉')) {
      return taskTypes.flowerBuilder
    }
  }

  if (breadcrumb[0] && breadcrumb[0].textContent.includes('畜禽產品')) {
    if (breadcrumb[1] && breadcrumb[1].textContent.includes('毛豬')) {
      return taskTypes.hogBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('羊')) {
      return taskTypes.ramBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('雞')) {
      return taskTypes.chickenBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('鴨')) {
      return taskTypes.duckBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('牛')) {
      return taskTypes.cattleBuilder
    }
  }

  if (breadcrumb[0] && breadcrumb[0].textContent.includes('漁產品')) {
    return taskTypes.originSeafoodBuilder
  }

  // 合計項目
  if (breadcrumb[0] && breadcrumb[0].textContent.includes('合計項目')) {
    if (breadcrumb[1] && breadcrumb[1].textContent.includes('蔬菜-批發合計')) {
      return taskTypes.cropBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('水果-批發合計')) {
      return taskTypes.fruitBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('花卉-批發合計')) {
      return taskTypes.flowerBuilder
    }

    if (breadcrumb[1] && breadcrumb[1].textContent.includes('毛豬合計')) {
      return taskTypes.hogBuilder
    }
  }

  <!-- 全品項查詢 -->
  if (breadcrumb[1] && breadcrumb[1].textContent.includes('全品項查詢')) {
    <!-- 糧價 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('糧價')) {
      return taskTypes.riceBuilder
    }

    <!-- 蔬菜-批發 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('蔬菜')) {
      return taskTypes.cropBuilder
    }

    <!-- 水果-批發 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('水果')) {
      return taskTypes.fruitBuilder
    }

    <!-- 花卉-批發 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('花卉')) {
      return taskTypes.flowerBuilder
    }

    <!-- 漁產品 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('漁產品')) {
      if (breadcrumb[3] && breadcrumb[3].textContent.includes('產地')) {
        return taskTypes.originSeafoodBuilder
      }

      if (breadcrumb[3] && breadcrumb[3].textContent.includes('批發')) {
        return taskTypes.wholesaleSeafoodBuilder
      }
    }

    <!-- 畜禽產品 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('毛豬')) {
      return taskTypes.hogBuilder
    }

    if (breadcrumb[2] && breadcrumb[2].textContent.includes('羊')) {
      return taskTypes.ramBuilder
    }

    if (breadcrumb[2] && breadcrumb[2].textContent.includes('雞')) {
      return taskTypes.chickenBuilder
    }

    if (breadcrumb[2] && breadcrumb[2].textContent.includes('鴨')) {
      return taskTypes.duckBuilder
    }

    if (breadcrumb[2] && breadcrumb[2].textContent.includes('鵝')) {
      return taskTypes.gooseBuilder
    }

    if (breadcrumb[2] && breadcrumb[2].textContent.includes('牛')) {
      return taskTypes.cattleBuilder
    }

    <!-- 飼料 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('飼料')) {
      return taskTypes.feedBuilder
    }

    <!-- 畜產會 -->
    if (breadcrumb[2] && breadcrumb[2].textContent.includes('畜產會')) {
      return taskTypes.naifChickensBuilder
    }
  }

  return {}
}

function clearCelerySchedule() {
  if (window.ajax && window.ajax.state() === 'pending') {
    window.ajax.abort()
  }
  if (window.interval) {
    console.log('clearInterval')
    clearInterval(window.interval)
    window.interval = undefined
  }

  if (window.prevState || window.currState) {
    window.prevState = undefined
    window.currState = undefined
  }
}

function refreshChart() {
  const chart = document.querySelector('#chart-functions-tab li.active a[data-load="true"]')
  const allCharts = [...document.querySelectorAll('#chart-functions-tab li a[data-load]')]

  chart?.parentElement?.classList.remove('active')
  chart?.setAttribute('data-load', '')

  for (let el of allCharts) {
    if (chart instanceof Element && el instanceof Element) {
      if (chart === el) {
        continue
      }

      el.setAttribute('data-load', '')
    }
  }

  chart?.click()
}

function updateCeleryScheduleUi() {
  document.querySelector('#update-time')?.remove()

  const chart1 = document.querySelector('#chart-1')

  if (!document.querySelector('h1.ajax-loading-animation') && window.celeryTask) {
    let isSuccess = window.celeryTask.state === 'SUCCESS'
    let updating = `<h5 style="padding: 1rem 2.5rem 0; font-weight: 500;" class="ajax-loading-animation"><i class="fa fa-cog fa-spin"></i> 資料更新中...</h5>`
    let nextTime = `<h5 style="padding: 1rem 2.5rem 0; font-weight: 500;">資料下次更新時間: ${window.celeryTask.nextTime}</h5>`
    let template = `
				  <div id="update-time" style="display: flex; justify-content: space-between; color: #9a0325;">
						<h5 style="padding: 1rem 2.5rem 0; font-weight: 500;">資料上次更新時間: ${window.celeryTask.succeeded}</h5>
						${isSuccess ? nextTime : updating}
					</div>
				  `

    const el = document.querySelector('#chart-functions-tab')
    if (el) {
      el.insertAdjacentHTML('afterend', template)
      console.log('updateCeleryScheduleUi')
    }
  }

  if (chart1 && chart1.checkVisibility() && chart1.querySelector('h1')) {
    document.querySelector('#update-time')?.remove()
  }
}

function showNotification() {
  const notyf = new Notyf({
    duration: 10000,
    position: {
      x: 'center',
      y: 'bottom',
    },
    types: [
      {
        type: 'success',
        dismissible: true
      }
    ]
  });
  const template = `
  <div style="margin-bottom: .5rem;">資料更新完成，是否刷新頁面?</div>
  <div style="width: 100%; max-width: 100%; text-align: center;">
  <span class="notif__message__yes">是</span>　•　<span class="notif__message__no">否</span>
  </div>
  `
  const notification = notyf.success(template);

  const dismiss = document.querySelector('.notyf__dismiss-btn')
  document.querySelector('.notif__message__yes').addEventListener('click', () => {
    dismiss.click()
    refreshChart()
  })

  document.querySelector('.notif__message__no').addEventListener('click', () => {
    dismiss.click()
  })
}

function getCelerySchedule() {
  window.ajax = $.ajax({
    url: '/get-celery-task-schedule/',
    type: 'POST',
    dataType: 'json',
    data: getTaskObj(),
    cache: true,
    async: true,
    beforeSend: function (xhr, settings) {
      // CSRF token
      if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
        xhr.setRequestHeader("X-CSRFToken", $.cookie('csrftoken'));
      }
    },
    success(data) {
      if (!data.error) {
        if (data.state === 'SUCCESS') {
          window.celeryTask = data

          if (window.interval === undefined) {
            console.log('setInterval')
            window.interval = setInterval(() => {
              getCelerySchedule()
            }, 1000 * 60)
          }
        }
        if (data.state === 'STARTED') {
          window.celeryTask = data

          if (window.interval === undefined) {
            console.log('setInterval')
            window.interval = setInterval(() => {
              getCelerySchedule()
            }, 1000 * 30)
          }
        }

        if (window.prevState === undefined && window.currState === undefined) {
          window.prevState = data.state
          window.currState = data.state
        } else {
          window.prevState = window.currState
          window.currState = data.state

          if (window?.prevState === 'STARTED' && window?.currState === 'SUCCESS') {
            // TODO: refresh chart
            showNotification()
          }
        }

        updateCeleryScheduleUi()
      } else {
        console.log(`response: ${data.error}`)
      }

    },
    error(xhr, status, error) {
      console.log(`error: ${error}`)
    }
  })
}