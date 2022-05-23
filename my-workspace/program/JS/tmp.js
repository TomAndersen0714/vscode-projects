window.addEventListener('keydown', e => {
    let video = document.querySelector('video');
    if (video && (e.key === '0')) video.playbackRate -= 0.25;
    else if (video && e.key === '1') video.playbackRate += 0.25;
})