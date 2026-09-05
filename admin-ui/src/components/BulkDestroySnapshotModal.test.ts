// @vitest-environment happy-dom
import { expect, it } from 'vite-plus/test'
import { shallowMount } from '@vue/test-utils'
import BulkDestroySnapshotModal from './BulkDestroySnapshotModal.vue'

it('requires the captured dataset name and blocks confirmation while deleting', async () => {
  const wrapper = shallowMount(BulkDestroySnapshotModal, {
    props: {
      open: true,
      host: 'mira',
      dataset: 'tank/data',
      snapshots: ['daily', 'weekly'],
      loading: false,
    },
    global: {
      stubs: {
        Modal: {
          props: ['description'],
          template: '<div>{{ description }}<slot name="body"/><slot name="footer"/></div>',
        },
        FormField: { template: '<div><slot/></div>' },
        Input: {
          props: ['modelValue'],
          template:
            '<input :value="modelValue" @input="$emit(\'update:modelValue\', $event.target.value)"/>',
        },
        Button: { props: ['disabled'], template: '<button :disabled="disabled"><slot/></button>' },
      },
    },
  })
  const confirm = wrapper.findAll('button')[1]!
  expect(wrapper.text()).toContain('mira')
  expect(wrapper.text()).toContain('tank/data@daily')
  expect(confirm.attributes('disabled')).toBeDefined()
  await wrapper.find('input').setValue('another/dataset')
  await wrapper.find('input').trigger('keydown.enter')
  expect(wrapper.emitted('confirm')).toBeUndefined()
  await wrapper.find('input').setValue('tank/data')
  expect(confirm.attributes('disabled')).toBeUndefined()
  await confirm.trigger('click')
  expect(wrapper.emitted('confirm')).toHaveLength(1)
  await wrapper.setProps({ loading: true })
  await wrapper.find('input').trigger('keydown.enter')
  expect(wrapper.emitted('confirm')).toHaveLength(1)
  wrapper.unmount()
})
